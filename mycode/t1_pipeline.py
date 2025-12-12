#t1.2
# import date and time for timestamps
from datetime import date,datetime
# basic stuff
from typing import List,Dict,Any
# Beautiful Soup for webscraping
from bs4 import BeautifulSoup
# fetch webpage html
import requests
# pandas for stats
import pandas as pd

# dict of tier 1 urls
T1_URLS = {
    'Manitoba EMO': 'https://www.gov.mb.ca/emo/index.html',
    'Manitoba Wildfire': 'https://www.gov.mb.ca/wildfire/index.html'
}

# define scraping method for T1 sources
def scrape_tier1_sources(urls: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """Scrape bulletin text from Tier 1 Manitoba wildfire sources."""
    scraped_data = {}

    for name, url in urls.items():
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            paragraphs = soup.find_all(['p', 'li', 'div'], string=True)
            text_blocks = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            full_text = "\n".join(text_blocks)

            scraped_data[name] = {
                'source_url': url,
                'source_name': name,
                'source_tier': 'Tier 1',
                'source_timestamp': datetime.utcnow().isoformat(),
                'raw_text': full_text
            }

            print(f" Scraped {name} successfully.")
        except requests.RequestException as e:
            print(f"Failed to scrape {name}: {e}")

    return scraped_data


## CALL
if __name__ == "__main__":
    results = scrape_tier1_sources(T1_URLS)
    print("Manitoba EMO data:")
    print(results['Manitoba EMO'])  # Replace 'data' with your actual dict variable
    print("\nManitoba Wildfire data:")
    print(results['Manitoba Wildfire'])
    print(f"Total records: {len(results['Manitoba EMO']) + len(results['Manitoba Wildfire'])}")

    print("Done. Keys:", list(results.keys()))