# T1
# import date and time for timestamps
from datetime import datetime, UTC
# basic stuff
from typing import Dict, Any, Tuple, List
# Beautiful Soup for webscraping
from bs4 import BeautifulSoup
# fetch webpage html
import requests
# pandas for stats
import pandas as pd


# dict of tier 1 urls
T1_URLS: Dict[str, str] = {
    "Manitoba Evacs": "https://www.manitoba.ca/wildfire/evacuations.html"
}


# define scraping method for T1 sources
def scrape_tier1_sources(urls: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
    rows: list[dict[str, Any]] = []
    all_raw_text: list[str] = []

    
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

            all_raw_text.append(f"===== {name} | {url} =====\n{full_text}\n")

            rows.append({
                "source_url": url,
                "source_name": name,
                "source_tier": 1,
                "source_timestamp": datetime.now(UTC).isoformat(),
            })

            print(f"Scraped {name} successfully.")
        except requests.RequestException as e:
            print(f"Failed to scrape {name}: {e}")

    return pd.DataFrame(rows), "\n".join(all_raw_text)


def scrape_wildfire_data(urls: Dict[str, str]) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []

    for name, url in urls.items():
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                if not rows:
                    continue

                headers = [
                    th.get_text(strip=True)
                    for th in rows[0].find_all(["th", "td"])
                ]

                # only keep evac tables
                # NOTE: header text must match page: "Date Evacuation Initiated"
                if "Local Authority" not in headers or "Date Evacuation Initiated" not in headers:
                    continue

                for tr in rows[1:]:
                    cols = tr.find_all("td")
                    if not cols:
                        continue

                    values = []
                    for td in cols:
                        text = td.get_text(strip=True)
                        span = int(td.get("colspan", 1))
                        values.extend([text] * span)

                    # skip pure section rows
                    if len(values) == 1 and values[0].lower() in {
                        "evacuation lifted",
                        "reopened",
                        "closed",
                    }:
                        continue

                    # pad if still short
                    if len(values) < len(headers):
                        values += [""] * (len(headers) - len(values))

                    values = values[:len(headers)]

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

    df = pd.DataFrame(records)

    

    # Return df
    return df


if __name__ == "__main__":
    # metdata and raw text
    results, raw_text = scrape_tier1_sources(T1_URLS)

    # view in terminal
    print(results.head())
    print(f"\nTotal records: {len(results)}")

    # Exporting data to CSV
    results.to_csv("T1_Data.csv", index=False)
    with open("T1_raw.txt", "w", encoding="utf-8") as f:
        f.write(raw_text)

    # structured wildfire evacuation info
    wildfire_df = scrape_wildfire_data(T1_URLS)
    
    
    # print out in terminal and export
    print(wildfire_df.head())
    print(f"\nWildfire evac records: {len(wildfire_df)}")
    wildfire_df.to_csv("T1_Wildfire_Evacs.csv", index=False)

    
