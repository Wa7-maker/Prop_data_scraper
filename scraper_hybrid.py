print(">>> SCRAPER FILE LOADED <<<")
print(">>> __name__ =", __name__)
print(">>> __file__ =", __file__)

import time
import random
import re
import sqlite3
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# ==================================================
# CONFIG
# ==================================================

AREAS = [
    {
        "province": "Western Cape",
        "city": "Cape Town",
        "area": "Milnerton",
        "url": "https://www.privateproperty.co.za/to-rent/western-cape/cape-town/milnerton/1399"
    }
]

DB_FILE = "privateproperty.db"

TEST_MODE = False
TEST_MAX_PAGES = 3
TEST_MAX_LISTINGS = 5

SEARCH_DELAY = (0.5, 1.0)
DETAIL_DELAY = (0.8, 1.5)

# ==================================================
# UTIL
# ==================================================

def parse_int(value):
    if not value:
        return None
    digits = re.findall(r"\d+", value.replace(" ", ""))
    return int("".join(digits)) if digits else None

# ==================================================
# DRIVER
# ==================================================

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    )
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

# ==================================================
# DATABASE
# ==================================================

import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "privateproperty.db")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        listing_id TEXT PRIMARY KEY,
        property_title TEXT,
        property_type TEXT,
        price TEXT,
        deposit_amount TEXT,
        bedrooms INTEGER,
        bathrooms INTEGER,
        floor_size TEXT,
        parking_spaces INTEGER,
        suburb TEXT,
        area TEXT,
        city TEXT,
        province TEXT,
        agent_name TEXT,
        estate_agency TEXT,
        description TEXT,
        available_from TEXT,
        listing_date TEXT,
        features_interior TEXT,
        features_exterior TEXT,
        features_security TEXT,
        features_utilities TEXT,
        features_lifestyle TEXT,
        url TEXT,
        first_seen TEXT,
        last_seen TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id TEXT,
        price TEXT,
        recorded_at TEXT
    )
    """)

    conn.commit()
    return conn


# ==================================================
# HTTP SESSION (COOKIES FROM SELENIUM)
# ==================================================

def build_http_session(driver):
    session = requests.Session()
    for c in driver.get_cookies():
        session.cookies.set(c["name"], c["value"])
    session.headers.update({
        "User-Agent": driver.execute_script("return navigator.userAgent;"),
        "Accept-Language": "en-ZA,en;q=0.9"
    })
    return session

# ==================================================
# SEARCH PAGE (SELENIUM)
# ==================================================

def scrape_search_page(driver, base_url, page):
    driver.get(f"{base_url}?page={page}")
    print("CURRENT URL:", driver.current_url)

    wait = WebDriverWait(driver, 10)

    try:
        cards = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.listing-result"))
        )


    except TimeoutException:
        print(f"Timeout waiting for listings on page {page}, retrying once...")
        time.sleep(3)
        cards = driver.find_elements(By.CSS_SELECTOR, "a.listing-result")
        if not cards:
            return []


    results = []

    for card in cards:
        try:
            url = card.get_attribute("href")
            if not url:
                continue

            def safe(css):
                try:
                    return card.find_element(By.CSS_SELECTOR, css).text.strip()
                except:
                    return None

            data = {
                "listing_id": url.rstrip("/").split("/")[-1],
                "url": url,
                "property_title": safe("div.listing-result__title"),
                "price": safe("div.listing-result__price"),
                "suburb": safe("span.listing-result__desktop-suburb"),
                "agent_name": safe("span.listing-result__agent-name"),
            }

            try:
                data["estate_agency"] = card.find_element(
                    By.CSS_SELECTOR, "img.listing-result__logo"
                ).get_attribute("alt")
            except:
                pass

            for f in card.find_elements(By.CSS_SELECTOR, "div.listing-result__features span"):
                title = f.get_attribute("title").lower()
                val = f.text.strip()
                if "bedroom" in title:
                    data["bedrooms"] = val
                elif "bathroom" in title:
                    data["bathrooms"] = val
                elif "parking" in title:
                    data["parking_spaces"] = val
                elif "floor" in title:
                    data["floor_size"] = val

            results.append(data)
        except:
            continue

    return results

# ==================================================
# DETAIL PAGE (HTTP)
# ==================================================

def scrape_detail_page(session, url):
    r = session.get(url, timeout=20)
    if r.status_code != 200:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    data = {}

    desc = soup.select_one("div.listing-description__text")
    if desc:
        data["description"] = desc.get_text(strip=True)

    for row in soup.select("div.listing-details__item"):
        label = row.select_one("span.listing-details__label")
        if not label:
            continue
        key = label.get_text(strip=True).lower()
        val = row.get_text(strip=True).split(":")[-1].strip()

        if key == "property type":
            data["property_type"] = val
        elif key == "listed":
            data["listing_date"] = val

    dep = soup.select_one("div.listing-price-display__additional-details")
    if dep and "deposit" in dep.get_text(strip=True).lower():
        data["deposit_amount"] = dep.get_text(strip=True).replace("Deposit:", "").strip()

    return data

# ==================================================
# UPSERT
# ==================================================

def upsert(conn, record, area_cfg):
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    record["province"] = area_cfg["province"]
    record["city"] = area_cfg["city"]
    record["area"] = area_cfg["area"]

    record["price_zar"] = parse_int(record.get("price"))
    record["deposit_zar"] = parse_int(record.get("deposit_amount"))
    record["floor_size_sqm"] = parse_int(record.get("floor_size"))
    record["bedrooms_int"] = parse_int(record.get("bedrooms"))
    record["bathrooms_int"] = parse_int(record.get("bathrooms"))
    record["parking_spaces_int"] = parse_int(record.get("parking_spaces"))

    cur.execute("SELECT first_seen FROM listings WHERE listing_id=?", (record["listing_id"],))
    row = cur.fetchone()

    if row:
        record["first_seen"] = row[0]
        record["last_seen"] = now
        cols = ", ".join(f"{k}=?" for k in record.keys())
        cur.execute(
            f"UPDATE listings SET {cols} WHERE listing_id=?",
            list(record.values()) + [record["listing_id"]]
        )
    else:
        record["first_seen"] = now
        record["last_seen"] = now
        cur.execute(
            f"INSERT INTO listings ({','.join(record.keys())}) VALUES ({','.join('?'*len(record))})",
            list(record.values())
        )

    conn.commit()

# ==================================================
# MAIN
# ==================================================

def main():
    driver = create_driver()
    print(">>> ENTERED MAIN <<<")

    
    conn = init_db()

    try:
        for area_cfg in AREAS:
            session = None
            listings_processed = 0

            for page in range(1, 999):
                if TEST_MODE and page > TEST_MAX_PAGES:
                    print("TEST_MODE: page limit reached")
                    break

                print(f"Scraping page {page}")
                results = scrape_search_page(driver, area_cfg["url"], page)
                print(f"Page {page}: found {len(results)} listings")

                if not results and page > 1:
                    print("No more pages detected.")
                    break

                if session is None:
                    session = build_http_session(driver)

                for listing in results:
                    detail = scrape_detail_page(session, listing["url"])
                    listing.update(detail)
                    upsert(conn, listing, area_cfg)

                    listings_processed += 1
                    time.sleep(random.uniform(*DETAIL_DELAY))

                    if TEST_MODE and listings_processed >= TEST_MAX_LISTINGS:
                        print("TEST_MODE: listing limit reached")
                        break   # <-- NOT return

                time.sleep(random.uniform(*SEARCH_DELAY))

    finally:
        driver.quit()
        conn.close()

if __name__ == "__main__":
    main()

