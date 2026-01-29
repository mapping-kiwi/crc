"""
TIER 1 (MANITOBA) WILDFIRE EVACUATION DATA SCRAPING PIPELINE

This python script scrapes official government websites 
for wildfire evacuation notices into a csv file and saves 
raw HTML text for manual reviewal.

ASSUMPTIONS:
1. The Manitoba evacuation page contains a table with headers:
   "Local Authority" and "Date Evacuation Initiated"
2. Blank "Local Authority" cells indicate merged cells and should
   inherit from the previous row (after filtering section headers)
3. "Local Authority" represents geographic entities mappable to 
   census boundaries (CSDs or First Nations)
4. Section header rows ("Evacuation Lifted", etc.) can be identified
   by their text content in the Local Authority column

KNOWN LIMITATIONS:
- Does not handle multiple tables per page with different schemas
- Forward-fill assumes chronological row order
- Name matching to census geographies not yet implemented
- No retry logic for failed requests

GIS CONSIDERATIONS:
- Output table must be joined to census boundaries by name
- "Local Authority" may include First Nations not in CSD dataset
- Requires manual QA of authority names before geocoding
"""

# Import Python libraries
from datetime import datetime, UTC
from typing import Dict, Any, Tuple, List
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os


# CONFIGURATION AND CONSTANTS
# Dictionary mapping source names to their government evacuation page URLs
T1_URLS: Dict[str, str] = {
    "Manitoba Evacs": "https://www.manitoba.ca/wildfire/evacuations.html"
}

# Define non-geographic labels that appear as section headers
NON_GEOGRAPHIC_LABELS = {
    "evacuation lifted", "reopened", "closed", 
    "evacuation order", "evacuation alert"
}

# Required table headers for validation
REQUIRED_HEADERS = ["Local Authority", "Date Evacuation Initiated"]


# QA SIGNAL TRACKING
class QASignals:
    """Track quality assurance metrics throughout the pipeline."""
    
    def __init__(self):
        self.signals = {
            "records_scraped": 0,
            "unique_authorities": 0,
            "records_with_dates": 0,
            "records_without_dates": 0,
            "non_geographic_rows_filtered": 0,
            "forward_filled_authorities": 0,
            "unique_event_ids": 0,
            "duplicate_event_ids": 0,
        }
        self.unmatched_authorities = []
    
    def update(self, key: str, value: int):
        """Update a QA signal."""
        self.signals[key] = value
    
    def increment(self, key: str, amount: int = 1):
        """Increment a QA signal."""
        self.signals[key] = self.signals.get(key, 0) + amount
    
    def report(self) -> str:
        """Generate a formatted QA report."""
        report = ["\n" + "="*60]
        report.append("QA SIGNALS - PIPELINE QUALITY METRICS")
        report.append("="*60)
        
        # Scraping metrics
        report.append("\n[SCRAPING METRICS]")
        report.append(f"  Records scraped: {self.signals['records_scraped']}")
        report.append(f"  Non-geographic rows filtered: {self.signals['non_geographic_rows_filtered']}")
        
        # Data quality metrics
        report.append("\n[DATA QUALITY]")
        report.append(f"  Unique authorities: {self.signals['unique_authorities']}")
        report.append(f"  Records with valid dates: {self.signals['records_with_dates']}")
        report.append(f"  Records without dates: {self.signals['records_without_dates']}")
        
        # Enrichment metrics
        report.append("\n[ENRICHMENT METRICS]")
        report.append(f"  Forward-filled authorities: {self.signals['forward_filled_authorities']}")
        report.append(f"  Unique event IDs generated: {self.signals['unique_event_ids']}")
        report.append(f"  Duplicate event IDs: {self.signals['duplicate_event_ids']}")
        
        # Calculate rates
        total = self.signals['records_scraped']
        if total > 0:
            date_coverage = (self.signals['records_with_dates'] / total) * 100
            report.append("\n[COVERAGE RATES]")
            report.append(f"  Date coverage: {date_coverage:.1f}% ({self.signals['records_with_dates']}/{total})")
        
        report.append("="*60 + "\n")
        return "\n".join(report)
    
    def save_report(self, filename: str):
        """Save QA report to file."""
        with open(filename, "w", encoding="utf-8") as f:
            f.write(self.report())
            
            # Add detailed signal data
            f.write("\n[RAW SIGNAL DATA]\n")
            for key, value in sorted(self.signals.items()):
                f.write(f"{key}: {value}\n")


# UTILITY FUNCTIONS
def ensure_directories():
    """Create necessary output directories if they don't exist."""
    os.makedirs("csv files", exist_ok=True)
    os.makedirs("raw_html", exist_ok=True)
    os.makedirs("raw_text", exist_ok=True)
    os.makedirs("qa_reports", exist_ok=True)


def audit_local_authorities(df: pd.DataFrame, qa: QASignals) -> pd.Series:
    """
    Check what types of entities appear in Local Authority column.
    Generate QA signals for authority quality.
    
    Args:
        df: DataFrame with Local Authority column
        qa: QA signals tracker
    
    Returns:
        Series with value counts of unique authorities
    """
    if "Local Authority" not in df.columns or df.empty:
        return pd.Series()
    
    unique_authorities = df["Local Authority"].value_counts()
    qa.update("unique_authorities", len(unique_authorities))
    
    # Flag suspicious entries
    suspicious = unique_authorities[
        unique_authorities.index.str.lower().str.contains(
            "evacuation|lifted|closed|reopened", 
            na=False
        )
    ]
    
    if len(suspicious) > 0:
        print("\n⚠️ WARNING: Non-geographic values found in Local Authority:")
        print(suspicious)
        print("\nThese may indicate section headers that weren't filtered correctly.\n")
    
    # Save detailed audit with frequency distribution
    audit_df = pd.DataFrame({
        'authority': unique_authorities.index,
        'record_count': unique_authorities.values,
        'percentage': (unique_authorities.values / len(df) * 100).round(2)
    })
    audit_df.to_csv("csv files/authority_audit.csv", index=False)
    
    return unique_authorities


# SCRAPING FUNCTIONS
def scrape_tier1_sources(urls: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
    """
    Fetch basic metadata from T1 sources and saves ALL raw HTML and text.
    
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
    rows: List[Dict[str, Any]] = []
    all_raw_text: List[str] = []
    
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    for name, url in urls.items():
        try:
            print(f"Fetching {name} from {url}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Save raw HTML (actual page source)
            html_filename = f"raw_html/{name.replace(' ', '_')}_{timestamp}.html"
            with open(html_filename, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"  → Saved raw HTML to {html_filename}")
            
            # Parse HTML content
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract all text within <p>, <li>, and <div> tags
            paragraphs = soup.find_all(["p", "li", "div"])
            text_blocks = [
                p.get_text(strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            ]
            full_text = "\n".join(text_blocks)

            # Store processed text for this source
            all_raw_text.append(f"===== {name} | {url} =====\n{full_text}\n")

            # Create metadata row
            rows.append({
                "source_url": url,
                "source_name": name,
                "source_tier": 1,
                "source_timestamp": datetime.now(UTC).isoformat(),
            })

            print(f"  ✓ Scraped {name} successfully.")
            
        except requests.RequestException as e:
            print(f"  ✗ Failed to scrape {name}: {e}")
    
    metadata_df = pd.DataFrame(rows)
    raw_text_combined = "\n".join(all_raw_text)
    
    return metadata_df, raw_text_combined


def scrape_wildfire_data(urls: Dict[str, str], qa: QASignals) -> pd.DataFrame:
    """
    Extract structured wildfire evacuation data from T1 sources.
    
    Focus on tables with specific headers "Local Authority",
    "Date Evacuation Initiated".
    
    Handle rowspan and colspan in HTML tables (merged cells).
    
    Returns:
        pd.DataFrame: DataFrame with structured wildfire evacuation data.
    """
    records: List[Dict[str, Any]] = []
    tables_found = 0
    tables_matched = 0

    for name, url in urls.items():
        try:
            print(f"\nExtracting structured data from {name}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            tables = soup.find_all("table")
            tables_found += len(tables)
            
            if not tables:
                print(f"  ⚠️ No tables found on page")
                continue

            for table in tables:
                rows = table.find_all("tr")
                if not rows:
                    continue

                # Extract headers from first row
                headers = [
                    th.get_text(strip=True)
                    for th in rows[0].find_all(["th", "td"])
                ]

                # Validate schema - only process evacuation tables
                if not all(req in headers for req in REQUIRED_HEADERS):
                    continue
                
                tables_matched += 1
                print(f"  ✓ Found evacuation table with {len(rows)-1} rows")

                active_rowspans = {}

                for tr in rows[1:]:  # Skip header row
                    cols = tr.find_all("td")
                    if not cols:
                        continue

                    values = []
                    col_idx = 0

                    # Fill from previous rowspans (merged cells)
                    while col_idx in active_rowspans:
                        values.append(active_rowspans[col_idx]["value"])
                        active_rowspans[col_idx]["rows_left"] -= 1
                        if active_rowspans[col_idx]["rows_left"] == 0:
                            del active_rowspans[col_idx]
                        col_idx += 1

                    # Process each cell
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

                    # Pad row if shorter than headers
                    if len(values) < len(headers):
                        values += [""] * (len(headers) - len(values))

                    # Trim if longer than headers
                    values = values[:len(headers)]

                    # Create row dictionary
                    row = dict(zip(headers, values))
                    
                    # CRITICAL: Filter non-geographic section headers
                    if row.get("Local Authority", "").lower() in NON_GEOGRAPHIC_LABELS:
                        qa.increment("non_geographic_rows_filtered")
                        continue
                    
                    # Add provenance metadata
                    row.update({
                        "source_url": url,
                        "source_name": name,
                        "source_tier": 1,
                        "source_timestamp": datetime.now(UTC).isoformat(),
                    })
                    
                    records.append(row)

            print(f"  ✓ Extracted {len([r for r in records if r['source_name'] == name])} records from {name}")
            
        except requests.RequestException as e:
            print(f"  ✗ Failed to scrape {name}: {e}")

    # Validate results
    if tables_found == 0:
        print("\n⚠️ WARNING: No tables found on any page")
    elif tables_matched == 0:
        print(f"\n⚠️ WARNING: Found {tables_found} table(s) but none matched required headers:")
        print(f"   Required: {REQUIRED_HEADERS}")
        print("   Page structure may have changed!")

    df = pd.DataFrame(records)
    qa.update("records_scraped", len(df))
    
    print(f"\n{'='*60}")
    print(f"Total evacuation records extracted: {len(df)}")
    print(f"{'='*60}")
    
    return df


# DATA CLEANING FUNCTIONS
def clean_wildfire_data(df: pd.DataFrame, qa: QASignals) -> pd.DataFrame:
    """
    Clean and enrich wildfire evacuation data.
    
    Args:
        df: Raw scraped DataFrame
    
    Returns:
        Cleaned DataFrame with parsed dates and event IDs
    """
    if df.empty:
        print("No data to clean")
        return df
    
    print("\nCleaning data...")
    
    # Forward-fill missing Local Authority names (for merged cells)
    # Only do this for rows with valid dates
    if "Local Authority" in df.columns and "Date Evacuation Initiated" in df.columns:
        # Count empty authorities before filling
        empty_before = df["Local Authority"].isna().sum() + (df["Local Authority"] == "").sum()
        
        df["Local Authority"] = df["Local Authority"].replace("", pd.NA)
        
        # Only forward-fill rows that have a date value
        mask = df["Date Evacuation Initiated"].notna() & (df["Date Evacuation Initiated"] != "")
        df.loc[mask, "Local Authority"] = df.loc[mask, "Local Authority"].ffill()
        
        # Count how many were filled
        empty_after = df["Local Authority"].isna().sum()
        filled = empty_before - empty_after
        qa.update("forward_filled_authorities", filled)
    
    # Parse dates into standardized format
    if "Date Evacuation Initiated" in df.columns:
        df["date_initiated_parsed"] = pd.to_datetime(
            df["Date Evacuation Initiated"], 
            errors='coerce'
        )
        
        records_with_dates = df["date_initiated_parsed"].notna().sum()
        records_without_dates = len(df) - records_with_dates
        
        qa.update("records_with_dates", records_with_dates)
        qa.update("records_without_dates", records_without_dates)
        
        # Generate unique event IDs
        df["event_id"] = (
            df["source_name"].fillna("unknown") + "_" +
            df["Local Authority"].fillna("unknown").str.replace(" ", "_") + "_" +
            df["date_initiated_parsed"].dt.strftime("%Y%m%d").fillna("unknown")
        ).str.lower()
        
        unique_events = df["event_id"].nunique()
        duplicate_events = len(df) - unique_events
        
        qa.update("unique_event_ids", unique_events)
        qa.update("duplicate_event_ids", duplicate_events)
    
    return df


# MAIN EXECUTION
if __name__ == "__main__":
    print("="*60)
    print("TIER 1 WILDFIRE EVACUATION DATA SCRAPING PIPELINE")
    print("="*60)
    
    # Initialize QA tracker
    qa = QASignals()
    
    # Create output directories
    ensure_directories()
    
    # Generate timestamp for this run
    run_timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    
    # STEP 1: Scrape metadata and raw content
    print("\n[STEP 1] Scraping metadata and raw content...")
    metadata_df, raw_text = scrape_tier1_sources(T1_URLS)
    
    if not metadata_df.empty:
        print(f"\nMetadata preview:")
        print(metadata_df)
        
        # Save metadata
        metadata_df.to_csv("csv files/T1_Data.csv", index=False)
        print(f"\n✓ Metadata saved to: csv files/T1_Data.csv")
        
        # Save processed text
        text_filename = f"raw_text/T1_raw_{run_timestamp}.txt"
        with open(text_filename, "w", encoding="utf-8") as f:
            f.write(raw_text)
        print(f"✓ Processed text saved to: {text_filename}")
    
    # STEP 2: Extract structured evacuation data
    print("\n[STEP 2] Extracting structured evacuation data...")
    wildfire_df = scrape_wildfire_data(T1_URLS, qa)
    
    if wildfire_df.empty:
        print("\n⚠️ No evacuation data extracted. Check webpage structure.")
    else:
        # STEP 3: Clean and enrich the data
        print("\n[STEP 3] Cleaning and enriching data...")
        wildfire_df = clean_wildfire_data(wildfire_df, qa)
        
        # STEP 4: Quality assurance
        print("\n[STEP 4] Running quality assurance checks...")
        audit_local_authorities(wildfire_df, qa)
        
        # STEP 5: Save outputs
        print("\n[STEP 5] Saving outputs...")
        
        # Save versioned output
        versioned_filename = f"csv files/T1_Wildfire_Evacs_{run_timestamp}.csv"
        wildfire_df.to_csv(versioned_filename, index=False)
        print(f"✓ Versioned output: {versioned_filename}")
        
        # Save latest (overwrites)
        wildfire_df.to_csv("csv files/T1_Wildfire_Evacs.csv", index=False)
        print(f"✓ Latest output: csv files/T1_Wildfire_Evacs.csv")
        
        # Display sample
        print("\nSample of extracted data:")
        print(wildfire_df.head())
        
        # STEP 6: Generate and display QA report
        print(qa.report())
        
        # Save QA report
        qa_filename = f"qa_reports/QA_Report_{run_timestamp}.txt"
        qa.save_report(qa_filename)
        print(f"✓ QA report saved to: {qa_filename}")
        
        print(f"\n{'='*60}")
        print(f"Pipeline completed successfully!")
        print(f"{'='*60}")