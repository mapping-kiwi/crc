# t1_pipeline.py

"""
TIER 1 (MANITOBA) WILDFIRE EVACUATION DATA SCRAPING PIPELINE

This python script scapes official government websites 
for wildfire evacuation notices into a csv file and saves 
raw HTML text for manual reviewal.

"""

# Import Python libraries
# date and time for timestamps
from datetime import datetime, UTC
# basic structures
from typing import Dict, Any, Tuple, List
# Beautiful Soup for webscraping
from bs4 import BeautifulSoup
# fetching webpage html
import requests
# pandas for stats
import pandas as pd


# READ CSV CENSUS DATA
# Read CSV file directly into a pandas DataFrame
df = pd.read_csv('csv files/Manitoba_2021_Census.csv')

# Display the first few rows of the data
print(df.head())


# Dictionary mapping source names to their government evacuation page URLs
# Keys = display name, Values = URLs to be scraped
T1_URLS: Dict[str, str] = {
    "Manitoba Evacs": "https://www.manitoba.ca/wildfire/evacuations.html"
}


# scraping method for T1 sources
def scrape_tier1_sources(urls: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
    """
        Fetch basic metadata from T1 sources and saves ALL raw text.
        
        Args:
            urls (Dict[str, str]): Mapping of source names to URLs.
        Returns:
            Tuple[pd.DataFrame, str]: DataFrame of metadata and raw text string.
        
        This creates a DataFrame with columns:
            - source_url
            - source_name
            - source_tier
            - source_timestamp
    """
    
    rows: list[dict[str, Any]] = [] # List stores one row per source scraped
    
    
    all_raw_text: list[str] = [] # Collects complete raw text from all sources

    
    # Looping though each source and its URL
    for name, url in urls.items():
        try:
            # Download webpage content
            print(f"Fetching {name} from {url}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse HTML content with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract all text within <p>, <li>, and <div> tags
            paragraphs = soup.find_all(["p", "li", "div"])
            text_blocks = [
                p.get_text(strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            ]
            full_text = "\n".join(text_blocks)

            # Store raw text for this source
            all_raw_text.append(f"===== {name} | {url} =====\n{full_text}\n")

            # Create metadata row
            rows.append({
                "source_url": url,
                "source_name": name,
                "source_tier": 1,
                "source_timestamp": datetime.now(UTC).isoformat(),
            })


            # Success message
            print(f"Scraped {name} successfully.")
        except requests.RequestException as e:
            # Catch errors and log failure
            print(f"Failed to scrape {name}: {e}")
    
    # Convert the list of dicts into pandas DataFrame
    metadata_df = pd.DataFrame(rows)
    raw_text_combined = "\n".join(all_raw_text)
    return metadata_df, raw_text_combined

#######################################################

def scrape_wildfire_data(urls: Dict[str, str]) -> pd.DataFrame:
    """
        Extract structured wildfire evacuation data from T1 sources.
        
        Focus on tables with specific headers "Local Authority",
        "Date Evacuation Initiated".
        
        Handle rowspan and colspan in HTML tables (merged cells).
        
        Return:
            pd.DataFrame: DataFrame with structured wildfire evacuation data.
    """
    records: List[Dict[str, Any]] = [] # to hold completed evac records


    # Loop through each source URL
    for name, url in urls.items():
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                if not rows: # skip empty tables
                    continue

                # Extract headers from first row
                headers = [
                    th.get_text(strip=True)
                    for th in rows[0].find_all(["th", "td"])
                ]

                # Only keep evac tables
                # NOTE: header text must match page: "Date Evacuation Initiated"
                if "Local Authority" not in headers or "Date Evacuation Initiated" not in headers:
                    continue

                active_rowspans = {}

                for tr in rows[1:]:
                    cols = tr.find_all("td")
                    if not cols:
                        continue

                    values = []
                    col_idx = 0

                    # fill from previous rowspans
                    while col_idx in active_rowspans:
                        values.append(active_rowspans[col_idx]["value"])
                        active_rowspans[col_idx]["rows_left"] -= 1
                        if active_rowspans[col_idx]["rows_left"] == 0:
                            del active_rowspans[col_idx]
                        col_idx += 1

                    for td in cols:
                        text = td.get_text(strip=True)
                        colspan = int(td.get("colspan", 1))
                        rowspan = int(td.get("rowspan", 1))

                        for _ in range(colspan):
                            values.append(text)

                            if rowspan > 1:
                                active_rowspans[col_idx] = {
                                    "value": text,
                                    "rows_left": rowspan - 1
                                }
                            col_idx += 1

                    # pad if short
                    if len(values) < len(headers):
                        values += [""] * (len(headers) - len(values))

                    values = values[:len(headers)]

                    # skip pure section rows
                    if len(values) == 1 and values[0].lower() in {
                        "evacuation lifted",
                        "reopened",
                        "closed",
                    }:
                        continue

                    row = dict(zip(headers, values))
                    row.update({
                        "source_url": url,
                        "source_name": name,
                        "source_tier": 1,
                        "source_timestamp": datetime.now(UTC).isoformat(),
                    })
                    records.append(row)


            print(f"Scraped {name} successfully.")
        except requests.RequestException as e:
            print(f"Failed to scrape {name}: {e}")

    # Final DataFrame
    df = pd.DataFrame(records)
    print(f"Total evacuation records scraped: {len(df)}")
    # Return df
    return df

# MAIN EXECUTION BLOCK
if __name__ == "__main__":
    print("Starting T1 Wildfire Evacuation Data Scraping Pipeline...")
    
    # Scrape metadata and raw text from all sources
    results, raw_text = scrape_tier1_sources(T1_URLS)

    # Preview in terminal
    print(results.head())
    print(f"\nTotal records: {len(results)}")

    # Exporting data to CSV
    results.to_csv("csv files/T1_Data.csv", index=False)
    with open("T1_raw.txt", "w", encoding="utf-8") as f:
        f.write(raw_text)

    # Extract structured wildfire evacuation info
    wildfire_df = scrape_wildfire_data(T1_URLS)

    # Data cleaning: forward fill missing Local Authority names
    if "Local Authority" in wildfire_df.columns:
        wildfire_df["Local Authority"] = wildfire_df["Local Authority"].replace("", pd.NA)
        wildfire_df["Local Authority"] = wildfire_df["Local Authority"].ffill()

    
    # Print out in terminal and export
    print(wildfire_df.head())
    print(f"\nWildfire evac records: {len(wildfire_df)}")
    wildfire_df.to_csv("csv files/T1_Wildfire_Evacs.csv", index=False)

    
