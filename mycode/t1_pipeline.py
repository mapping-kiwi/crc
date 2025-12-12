# T1
# import date and time for timestamps
from datetime import datetime, UTC
# basic stuff
from typing import Dict, Any
# Beautiful Soup for webscraping
from bs4 import BeautifulSoup
# fetch webpage html
import requests
# pandas for stats
import pandas as pd

# dict of tier 1 urls
T1_URLS: Dict[str, str] = {
    "Manitoba Evacs": "https://www.manitoba.ca/wildfire/evacuations.html",
}

# define scraping method for T1 sources
def scrape_tier1_sources(urls: Dict[str, str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for name, url in urls.items():
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            paragraphs = soup.find_all(["p", "li", "div"])
            text_blocks = [
                p.get_text(strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            ]
            full_text = "\n".join(text_blocks)

            rows.append({
                "source_url": url,
                "source_name": name,
                "source_tier": 1,
                "source_timestamp": datetime.now(UTC).isoformat(),
                "raw_text": full_text,
            })

            print(f"Scraped {name} successfully.")
        except requests.RequestException as e:
            print(f"Failed to scrape {name}: {e}")

    return pd.DataFrame(rows)

if __name__ == "__main__":
    results = scrape_tier1_sources(T1_URLS)

    # view in terminal
    print(results.head())
    print(f"\nTotal records: {len(results)}")

    # Exporting data to CSV
    results.to_csv("T1_Data.csv", index=False)
