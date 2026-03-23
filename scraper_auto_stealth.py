import os
import re
import time
import json
import random
import urllib.parse
from bs4 import BeautifulSoup
from seleniumbase import SB

DB_FILE = 'database.jsonl'

# TODO: Move to .env in production
PROXY_USER = "YOUR_PROXY_USER"
PROXY_PASS = "YOUR_PROXY_PASS"
PROXY_HOST = "YOUR_PROXY_HOST"
START_PORT = 10000
MAX_PORT = 10999

def load_db():
    print(f"[INFO] Scanning database: {DB_FILE}...")
    db = {"brands": {}, "scraped_data": {}}
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if entry["type"] == "brand_info": 
                        db["brands"][entry["name"]] = entry["data"]
                    elif entry["type"] == "perfume_data": 
                        db["scraped_data"][entry["brand"]] = entry["items"]
                except json.JSONDecodeError:
                    continue
    return db

def save_entry(entry_type, key, value):
    entry = {"type": entry_type}
    if entry_type == "brand_info":
        entry["name"] = key
        entry["data"] = value
    elif entry_type == "perfume_data":
        entry["brand"] = key
        entry["items"] = value
    with open(DB_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def create_proxy_extension(host, port, user, password):
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Auto Proxy Auth",
        "permissions": ["proxy", "tabs", "unlimitedStorage", "storage", "<all_urls>", "webRequest", "webRequestBlocking"],
        "background": {"scripts": ["background.js"]},
        "minimum_chrome_version":"22.0.0"
    }
    """
    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: { singleProxy: { scheme: "http", host: "%s", port: parseInt(%s) }, bypassList: ["localhost"] }
          };
    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    function callbackFn(details) {
        return { authCredentials: { username: "%s", password: "%s" } };
    }
    chrome.webRequest.onAuthRequired.addListener(
            callbackFn, {urls: ["<all_urls>"]}, ['blocking']
    );
    """ % (host, port, user, password)
    
    ext_dir = "proxy_auth_plugin"
    os.makedirs(ext_dir, exist_ok=True)
    with open(os.path.join(ext_dir, "manifest.json"), "w") as f:
        f.write(manifest_json)
    with open(os.path.join(ext_dir, "background.js"), "w") as f:
        f.write(background_js)
    return ext_dir

# Note: parse_perfume_card function is identical to scraper_manual_turbo.py
# (Omitted here for brevity, ensure you copy it from the turbo script)

def phase_2_scrape_perfumes(sb, db, limit=20):
    processed_in_session = 0
    total_to_do = len(db["brands"])
    already_done = len(db["scraped_data"])

    for brand_name, info in db["brands"].items():
        if brand_name in db["scraped_data"]:
            continue
            
        if processed_in_session >= limit:
            print(f"[INFO] Session limit reached ({limit}). Rotating proxy IP...")
            return "ROTATE"

        print(f"[INFO] Processing brand: {brand_name.upper()}")
        brand_slug_match = re.search(r'/designers/([^/]+)\.html', info['url'])
        brand_slug = brand_slug_match.group(1).lower() if brand_slug_match else brand_name.replace(' ', '-').lower()
        brand_slug = urllib.parse.unquote(brand_slug)
        
        empty_retries = 0
        while True:
            try:
                sb.open(info['url']) 
                
                try: sb.wait_for_element('a[href*="/perfume/"]', timeout=5)
                except Exception: pass 

                soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
                all_links = soup.find_all('a', href=re.compile(r'/perfume/[^/]+/[^/]+\.html'))
                
                perfumes = []
                unique_urls = set()
                for link in all_links:
                    p_url = "https://www.fragrantica.ru" + link.get('href') if link.get('href').startswith('/') else link.get('href')
                    if f"/perfume/{brand_slug}/" not in urllib.parse.unquote(p_url).lower(): continue
                    if p_url in unique_urls: continue
                    unique_urls.add(p_url)
                    
                    # Assuming parse_perfume_card is defined in the file
                    # data = parse_perfume_card(link, brand_name) 
                    # if data["name"]: perfumes.append({**data, "url": p_url})
                
                actual = len(perfumes)
                
                if actual == 0:
                    try:
                        page_src = sb.get_page_source().lower()
                        page_title = sb.get_title().lower()
                    except Exception:
                        return "ROTATE"

                    if "too many requests" in page_title or "just a moment" in page_title or "john wick" in page_src:
                        print("[WARNING] 429 / Cloudflare block. Sleeping 30s before IP rotation...")
                        time.sleep(30)
                        return "ROTATE"
                    
                    if empty_retries < 2:
                        empty_retries += 1
                        time.sleep(5)
                        continue
                    else:
                        print("[WARNING] Page is consistently empty. Forcing IP rotation.")
                        return "ROTATE"

                db["scraped_data"][brand_name] = perfumes
                # save_entry("perfume_data", brand_name, perfumes) 
                already_done += 1
                processed_in_session += 1
                
                print(f"[SUCCESS] Scraped {actual} items | Progress: {already_done}/{total_to_do}")
                
                # Simulating human behavior
                delay = random.uniform(1.0, 3.0)
                try:
                    sb.execute_script(f"window.scrollBy(0, {random.randint(200, 500)});")
                    time.sleep(random.uniform(0.3, 0.7))
                    sb.mouse_move("a[href*='/perfume/']", timeout=1) 
                except Exception: pass
                time.sleep(delay)
                break 

            except Exception as e:
                print(f"[ERROR] Load failed: {e}. Rotating IP...")
                return "ROTATE"
                
    return "FINISHED"

def main():
    print("[INFO] System initializing in AUTO STEALTH mode...")
    db = load_db()
    current_port = START_PORT
    
    while True:
        print(f"[INFO] Generating proxy config for port: {current_port}")
        ext_dir = create_proxy_extension(PROXY_HOST, current_port, PROXY_USER, PROXY_PASS)

        try:
            with SB(uc=True, extension_dir=ext_dir, headless=False, block_images=True) as sb:
                sb.driver.set_page_load_timeout(30)
                status = phase_2_scrape_perfumes(sb, db, limit=20)
                
            if status == "FINISHED":
                print("\n[INFO] Scraping process fully completed.")
                break
                
        except Exception as e:
            print(f"[ERROR] Session failure on port {current_port}: {e}")
        
        current_port = current_port + 1 if current_port < MAX_PORT else START_PORT
        time.sleep(2)

if __name__ == "__main__":
    main()