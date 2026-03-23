import os
import re
import time
import json
import socket
import urllib.parse
from bs4 import BeautifulSoup
from seleniumbase import SB

DB_FILE = 'database.jsonl'

# TODO: Move to .env in production
TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

def is_internet_available():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        import requests
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=5)
    except Exception as e:
        print(f"[ERROR] Failed to send Telegram alert: {e}")

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
                    
    total = len(db["brands"])
    done = len(db["scraped_data"])
    total_perfumes = sum(len(items) for items in db["scraped_data"].values())
    
    print(f"[INFO] DB loaded. Brands total: {total} | Scraped: {done} | Perfumes: {total_perfumes}")
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

def parse_perfume_card(link, brand_name):
    title_tag = link.find(class_=re.compile(r'perfume-title'))
    if title_tag:
        name = title_tag.get_text(strip=True)
        year_tag = link.find(class_=re.compile(r'year-badge'))
        year = year_tag.get_text(strip=True) if year_tag else None
        gender = "Not specified"
        
        for span in link.find_all('span'):
            text = span.get_text(strip=True).lower()
            if text in ['женский', 'мужской', 'унисекс', 'для женщин', 'для мужчин', 'для мужчин и женщин']:
                if 'жен' in text: gender = 'Female'
                elif 'муж' in text: gender = 'Male'
                else: gender = 'Unisex'
                break
        return {"name": name, "year": year, "gender": gender}
    
    raw_name = link.get_text(strip=True)
    if not raw_name:
        img = link.find('img')
        raw_name = img.get('alt') if img else ""
    
    year, gender = None, None
    match = re.search(r'((?:19|20)\d{2})?(женский|мужской|унисекс)?(\d*)$', raw_name, re.IGNORECASE)
    if match:
        year = match.group(1)
        gender_raw = match.group(2)
        if gender_raw:
            if 'жен' in gender_raw.lower(): gender = 'Female'
            elif 'муж' in gender_raw.lower(): gender = 'Male'
            else: gender = 'Unisex'
        clean_name = raw_name[:match.start()].strip()
    else:
        clean_name = raw_name.strip()
    
    clean_name = re.sub(re.escape(brand_name) + r'\s*$', '', clean_name, flags=re.IGNORECASE).strip()
    if not clean_name:
        clean_name = brand_name
        
    return {"name": re.sub(r'\s+', ' ', clean_name), "year": year, "gender": gender if gender else "Not specified"}

def phase_2_scrape_perfumes(sb, db):
    total_to_do = len(db["brands"])
    already_done = len(db["scraped_data"])
    total_perfumes = sum(len(items) for items in db["scraped_data"].values())

    for brand_name, info in db["brands"].items():
        if brand_name in db["scraped_data"]:
            continue

        print(f"[INFO] Processing brand: {brand_name.upper()}")
        brand_slug_match = re.search(r'/designers/([^/]+)\.html', info['url'])
        brand_slug = brand_slug_match.group(1).lower() if brand_slug_match else brand_name.replace(' ', '-').lower()
        brand_slug = urllib.parse.unquote(brand_slug)
        
        empty_retries = 0
        while True:
            try:
                start_time = time.time()
                sb.open(info['url']) 
                
                try:
                    sb.wait_for_element('a[href*="/perfume/"]', timeout=4)
                except Exception:
                    pass 

                soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
                all_links = soup.find_all('a', href=re.compile(r'/perfume/[^/]+/[^/]+\.html'))
                
                perfumes = []
                unique_urls = set()
                for link in all_links:
                    p_url = "https://www.fragrantica.ru" + link.get('href') if link.get('href').startswith('/') else link.get('href')
                    if f"/perfume/{brand_slug}/" not in urllib.parse.unquote(p_url).lower():
                        continue
                    if p_url in unique_urls:
                        continue
                    
                    unique_urls.add(p_url)
                    data = parse_perfume_card(link, brand_name)
                    if data["name"]:
                        perfumes.append({**data, "url": p_url})
                
                actual = len(perfumes)
                
                if actual == 0:
                    try:
                        page_src = sb.get_page_source().lower()
                        page_title = sb.get_title().lower()
                    except Exception:
                        print("[WARNING] Browser window closed unexpectedly. Restarting session...")
                        return "RESTART"

                    is_blocked = ("too many requests" in page_title or 
                                  "just a moment" in page_title or 
                                  "john wick" in page_src)

                    if is_blocked:
                        print("\n[CRITICAL] 429 Too Many Requests or Cloudflare block detected.")
                        send_telegram_alert("[ALERT] Parser stopped. 429 Error. Manual VPN switch required.")
                        input("[ACTION] Switch your VPN node and press [ENTER] to continue...")
                        continue
                    
                    if empty_retries < 2:
                        print("[WARNING] Page loaded empty. Retrying...")
                        empty_retries += 1
                        time.sleep(1)
                        continue
                    else:
                        print("[WARNING] Page is consistently empty.")
                        ans = input("[ACTION] Press [ENTER] to retry, or type 's' to skip this brand: ")
                        if ans.lower().strip() == 's':
                            print(f"[INFO] Skipping {brand_name}.")
                            break 
                        else:
                            empty_retries = 0
                            continue

                db["scraped_data"][brand_name] = perfumes
                save_entry("perfume_data", brand_name, perfumes) 
                already_done += 1
                total_perfumes += actual
                
                elapsed = time.time() - start_time
                print(f"[SUCCESS] Scraped {actual} items in {elapsed:.1f}s | Progress: {already_done}/{total_to_do} | Total items: {total_perfumes}")
                
                if already_done % 50 == 0:
                    send_telegram_alert(f"[STATUS] Scraper running. Brands: {already_done}/{total_to_do}. Items: {total_perfumes}.")
                
                break 

            except Exception as e:
                print(f"[ERROR] Page load failed: {e}")
                input("[ACTION] Check your connection/VPN and press [ENTER] to retry...")
                continue
                
    return "FINISHED"

def main():
    print("[INFO] System initializing...")
    db = load_db()
    send_telegram_alert("[INFO] Scraper started in MANUAL TURBO mode.")
    
    while True:
        try:
            with SB(uc=True, headless=False, block_images=True, page_load_strategy="eager") as sb:
                sb.driver.set_page_load_timeout(10)
                status = phase_2_scrape_perfumes(sb, db)
                
            if status == "FINISHED":
                print("\n[INFO] Scraping process completed successfully.")
                send_telegram_alert("[INFO] Parsing process fully completed.")
                break
                
        except Exception as e:
            print(f"[CRITICAL] Session failure: {e}. Restarting browser instance...")
            time.sleep(2)

if __name__ == "__main__":
    main()