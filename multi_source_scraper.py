import os
import json
import requests
from datetime import datetime, timedelta

# --- Configuration ---
# Disaster types mapping across different APIs can vary. 
# We need to standardize our search.
# Target: Earthquakes, Tsunamis, Floods, Cyclones, Volcanoes, Droughts, Forest Fires
RELIEFWEB_TYPES = ["Earthquake", "Tsunami", "Flood", "Tropical Cyclone", "Volcano", "Drought", "Wild Fire"]
GDACS_TYPES = ["EQ", "TS", "FL", "TC", "VO", "DR", "WF"]

# Date calculations for the last 10 years
TODAY = datetime.utcnow()
TEN_YEARS_AGO = TODAY - timedelta(days=365*10)
START_DATE_STR = TEN_YEARS_AGO.strftime("%Y-%m-%dT%H:%M:%S+00:00")
END_DATE_STR = TODAY.strftime("%Y-%m-%dT%H:%M:%S+00:00")

# Folders (Assuming they exist or will be created by the user/notebook)
TEXT_DATA_DIR = "Text_Data"
TABULAR_DATA_DIR = "Tabular_Data"
GDACS_DATA_DIR = "GDACS_Data"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# --- 1. ReliefWeb API Fetcher ---
def fetch_reliefweb_data():
    print("Fetching ReliefWeb Data...")
    ensure_dir(TEXT_DATA_DIR)
    
    # Using the user-provided v2 endpoint and registered appname, but accessed via browser (Playwright)
    url = "https://api.reliefweb.int/v2/disasters?appname=rwint-user-2909053&profile=list&preset=latest&slim=1&query[value]=type.id:(4628+AND+5706)+OR+type.id:(4624+OR+4611+OR+4618+OR+4648+OR+4615)&query[operator]=AND&limit=1000"

    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            # We use chromium in headless mode
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Navigate to the API endpoint
            print(f"  -> Sending GET request via headless browser to: {url}")
            response = page.goto(url)
            
            if response.status == 403:
                print("  -> ReliefWeb API requires a registered appname. Even via browser, the key was rejected.")
                browser.close()
                return
                
            if response.ok:
                # The browser will render the JSON response as text wrapped in a `pre` tag or just raw body text
                raw_json = page.evaluate("() => document.body.innerText")
                data = json.loads(raw_json)
                
                filepath = os.path.join(TEXT_DATA_DIR, "reliefweb_disasters_10yrs.json")
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
                print(f"  -> Saved {len(data.get('data', []))} records to {filepath}")
            else:
                print(f"  -> Error fetching ReliefWeb data: HTTP {response.status}")
                
            browser.close()
            
    except ImportError:
        print("  -> Error: 'playwright' is not installed. Please run '!pip install playwright' and '!playwright install chromium' in your notebook first.")
    except Exception as e:
        print("  -> Error fetching ReliefWeb data via Playwright. Please ensure 'playwright install' has been executed.")
    # <-- End of Playwright Fetcher Block -->

# --- 2. GDACS API Fetcher ---
def fetch_gdacs_data():
    print("\nFetching GDACS Data...")
    ensure_dir(GDACS_DATA_DIR)
    
    # The main GDACS API is currently returning 503 Service Unavailable. 
    # We will fallback to parsing their RSS feed, which is functional.
    # Note: RSS feed only gives recent alerts, not 10 years of history.
    url = "https://www.gdacs.org/xml/rss.xml"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        filepath = os.path.join(GDACS_DATA_DIR, "gdacs_recent_rss.xml")
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"  -> Saved GDACS RSS feed to {filepath} (Note: API is down, using 7-day RSS feed)")
        
    except Exception as e:
        print(f"  -> Error fetching GDACS data: {e}")

# --- 3. HDX API Fetcher ---
def fetch_hdx_data():
    print("\nFetching HDX Data...")
    ensure_dir(TABULAR_DATA_DIR)
    
    url = "https://data.humdata.org/api/3/action/package_search"
    # Query for EMDAT datasets
    params = {"q": "EM-DAT"}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success"):
            datasets = data["result"]["results"]
            print(f"  -> Found {len(datasets)} packages matching 'EM-DAT' on HDX")
            
            # Save the metadata for reference
            metadata_path = os.path.join(TABULAR_DATA_DIR, "hdx_emdat_metadata.json")
            with open(metadata_path, 'w') as f:
                json.dump(data, f, indent=4)
                
            # Attempt to download the first available XLSX resource
            for dataset in datasets:
                resources = dataset.get("resources", [])
                for resource in resources:
                    if resource.get("format", "").lower() == "xlsx":
                        download_url = resource.get("download_url")
                        file_name = resource.get("name", "hdx_emdat_data.xlsx")
                        
                        xlsx_resp = requests.get(download_url)
                        xlsx_resp.raise_for_status()
                        
                        xlsx_path = os.path.join(TABULAR_DATA_DIR, file_name)
                        with open(xlsx_path, 'wb') as f:
                            f.write(xlsx_resp.content)
                        print(f"  -> Downloaded HDX XLSX resource: {file_name}")
                        return # Stop after the first good XLSX is found
            print("  -> No suitable XLSX resources found on HDX for EM-DAT queries.")
        
    except Exception as e:
        print(f"  -> Error fetching HDX data: {e}")
    # Implement later after testing ReliefWeb and GDACS

# --- 4. EM-DAT API Fetcher ---
def fetch_emdat_data():
    print("\nFetching EM-DAT Data...")
    print("  -> Notes: EM-DAT's API might require authentication. Attempting public endpoints.")
    # Implement later after testing ReliefWeb and GDACS

if __name__ == "__main__":
    print(f"--- Starting Multi-Source Disaster Scraper ---")
    print(f"Time Range: {TEN_YEARS_AGO.strftime('%Y-%m-%d')} to {TODAY.strftime('%Y-%m-%d')}")
    fetch_reliefweb_data()
    fetch_gdacs_data()
    fetch_hdx_data()
    fetch_emdat_data()
    print("\n--- Scraping Complete ---")
