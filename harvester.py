import asyncio
import aiohttp
import lxml.etree as ET
import re
import json
import csv
import os
import logging
import gzip

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def fetch_url(session, url, semaphore):
    """Fetches a URL with semaphore control."""
    async with semaphore:
        try:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    logger.warning(f"Failed to fetch {url}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

async def parse_sitemap_for_urls(session, url, semaphore):
    """Parses a sitemap XML and yields sub-sitemap URLs or final URLs."""
    content = await fetch_url(session, url, semaphore)
    if not content:
        return False, []

    try:
        if url.endswith('.gz'):
            content = gzip.decompress(content)

        root = ET.fromstring(content)

        is_index = False
        if root.tag.endswith('sitemapindex'):
            is_index = True

        urls = []
        for elem in root:
            for child in elem:
                if 'loc' in child.tag:
                    urls.append(child.text)

        return is_index, urls
    except Exception as e:
        logger.error(f"Error parsing sitemap {url}: {e}")
        return False, []

async def extract_metadata(session, url, semaphore):
    """Extracts metadata from a Repl URL using __NEXT_DATA__."""
    content = await fetch_url(session, url, semaphore)
    if not content:
        return None

    try:
        html = content.decode('utf-8')
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            data = json.loads(match.group(1))
            apollo = data.get("props", {}).get("apolloState", {})

            # Find the true Repl reference from ROOT_QUERY if possible
            root_query = apollo.get("ROOT_QUERY", {})
            target_repl_key = None

            # Look for getRepl(...) in ROOT_QUERY
            for key, val in root_query.items():
                if key.startswith("getRepl(") and val and "__ref" in val:
                    target_repl_key = val["__ref"]
                    break

            # If not found in ROOT_QUERY, fallback to finding the first Repl:
            if not target_repl_key:
                for key, val in apollo.items():
                    if key.startswith("Repl:"):
                        target_repl_key = key
                        break

            if not target_repl_key or target_repl_key not in apollo:
                return None

            repl_val = apollo[target_repl_key]

            # Now find the owner
            owner_ref = repl_val.get("owner", {}).get("__ref")
            author_username = None
            if owner_ref and owner_ref in apollo:
                author_username = apollo[owner_ref].get("username")

            language = repl_val.get("language")
            tags_raw = repl_val.get("tags", [])
            tags = []
            for t in tags_raw:
                if isinstance(t, dict) and "__ref" in t:
                    # Resolve tag ref if possible
                    tag_obj = apollo.get(t["__ref"])
                    if tag_obj and "id" in tag_obj:
                        tags.append(tag_obj["id"])
                    elif tag_obj and "name" in tag_obj:
                        tags.append(tag_obj["name"])
                elif isinstance(t, str):
                    tags.append(t)

            return {
                "url": url,
                "title": repl_val.get("title"),
                "description": repl_val.get("description"),
                "author_username": author_username,
                "language": language,
                "tags": ",".join(tags)
            }
    except Exception as e:
        logger.debug(f"Error extracting metadata from {url}: {e}")

    return None

def is_valuable_repl(data):
    """Filters out empty or default descriptions."""
    if not data:
        return False

    description = data.get("description")
    if not description:
        return False

    description = description.strip()
    if not description:
        return False

    default_patterns = [
        r"a new (python|node\.js|html|cpp|java|ruby) repl",
        r"^test$",
        r"^default$"
    ]

    for pattern in default_patterns:
        if re.search(pattern, description, re.IGNORECASE):
            return False

    return True

def save_to_csv(data_batch, filename="replit_links_v1.csv"):
    """Saves a batch of data to CSV."""
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        fieldnames = ["url", "title", "description", "author_username", "language", "tags"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(data_batch)

def load_state(filename="state.json", csv_filename="replit_links_v1.csv"):
    """Loads checkpoint state and counts currently harvested links."""
    harvested_count = 0
    if os.path.exists(csv_filename):
        with open(csv_filename, 'r', encoding='utf-8') as f:
            harvested_count = sum(1 for line in f) - 1 # Exclude header
            if harvested_count < 0: harvested_count = 0

    if os.path.exists(filename):
        with open(filename, 'r') as f:
            state = json.load(f)
            state["harvested_count"] = harvested_count
            return state
    return {"visited_sitemaps": [], "processed_urls": [], "sitemaps_to_visit": ["https://replit.com/sitemap.xml"], "harvested_count": harvested_count}

def save_state(state, filename="state.json"):
    """Saves checkpoint state."""
    with open(filename, 'w') as f:
        json.dump(state, f)

async def harvest_data():
    """Main execution function."""
    semaphore = asyncio.Semaphore(20)
    state = load_state()

    to_visit = state["sitemaps_to_visit"]
    visited_sitemaps = set(state["visited_sitemaps"])
    unique_urls = set(state["processed_urls"])
    data_batch = []

    # Priority queues:
    to_visit.sort(key=lambda x: 0 if "templates" in x or "team-profiles" in x else 1)

    async with aiohttp.ClientSession() as session:
        while to_visit:
            current_sitemap = to_visit[0]
            if current_sitemap in visited_sitemaps:
                to_visit.pop(0)
                continue

            logger.info(f"Processing sitemap: {current_sitemap}")

            is_index, urls = await parse_sitemap_for_urls(session, current_sitemap, semaphore)

            if is_index:
                for url in urls:
                    if url.endswith('.xml') or url.endswith('.xml.gz'):
                        if url not in to_visit and url not in visited_sitemaps:
                            to_visit.append(url)
                # Re-sort to maintain priority after adding new sitemaps
                to_visit.sort(key=lambda x: 0 if "templates" in x or "team-profiles" in x else 1)

                # We finished fully processing this index sitemap, so we can save it safely
                visited_sitemaps.add(current_sitemap)
                state["visited_sitemaps"] = list(visited_sitemaps)
                state["sitemaps_to_visit"] = to_visit
                save_state(state)
            else:
                target_urls = [u for u in urls if "/@" in u and u not in unique_urls]

                # Process target URLs in batches
                batch_size = 100
                for i in range(0, len(target_urls), batch_size):
                    batch_urls = target_urls[i:i+batch_size]

                    tasks = [extract_metadata(session, url, semaphore) for url in batch_urls]
                    results = await asyncio.gather(*tasks)

                    for url, data in zip(batch_urls, results):
                        if url not in unique_urls:
                            state["processed_urls"].append(url)
                            unique_urls.add(url) # Add to processed list

                            if is_valuable_repl(data):
                                data_batch.append(data)
                                state["harvested_count"] += 1
                                logger.info(f"Harvested valuable Repl: {data.get('title')} ({url}). Total: {state['harvested_count']}")

                                if len(data_batch) >= 5000:
                                    # Atomic flush: write data, then save state
                                    save_to_csv(data_batch)
                                    data_batch.clear()
                                    save_state(state)

                    if state.get("harvested_count", 0) >= 100000:
                        logger.info("Target of 100,000 URLs reached!")
                        break

                # We finished processing this sitemap
                visited_sitemaps.add(current_sitemap)
                state["visited_sitemaps"] = list(visited_sitemaps)
                state["sitemaps_to_visit"] = to_visit
                save_state(state)

            if state.get("harvested_count", 0) >= 100000:
                break

        # Save any remaining data at the very end
        if data_batch:
            save_to_csv(data_batch)
            data_batch.clear()
            save_state(state)

if __name__ == "__main__":
    asyncio.run(harvest_data())
