"""
CLEANING MODULE FOR WILDFIRE EVACUATION ETL PIPELINE

Handles data normalization, filtering, and date parsing for census and wildfire data.
Part of the TRANSFORM stage in the ETL process.

Functions:
    - normalize_name: Standardize place names for matching
    - filter_non_geographic_rows: Remove section headers from wildfire data
    - parse_evacuation_dates: Standardize date formats
    - clean_wildfire_data: Complete cleaning pipeline for evacuation data
    - clean_census_data: Prepare census lookup table
"""

import pandas as pd
from datetime import datetime
from typing import Set


# CONFIGURATION
NON_GEOGRAPHIC_LABELS: Set[str] = {
    "evacuation lifted", 
    "reopened", 
    "closed", 
    "evacuation order", 
    "evacuation alert"
}


def normalize_name(name: str) -> str:
    """
    Standardize place names for better matching across datasets.
    
    Removes common prefixes (Town of, City of, RM of, etc.),
    converts to lowercase, and removes extra whitespace.
    
    Args:
        name (str): Original place name
        
    Returns:
        str: Normalized place name
        
    Examples:
        >>> normalize_name("Town of Flin Flon")
        'flin flon'
        >>> normalize_name("City of Thompson  ")
        'thompson'
    """
    if not isinstance(name, str):
        return ""
    
    name = name.lower().strip()
    
    # Common administrative prefixes to strip
    prefixes = [
        "town of ", 
        "city of ", 
        "rm of ", 
        "r.m. of ", 
        "rural municipality of ",
        "municipality of ",
        "village of ",
        "northern village of "
    ]
    
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break  # Only remove first matching prefix
    
    # Clean up extra spaces
    name = " ".join(name.split())
    
    return name


def filter_non_geographic_rows(df: pd.DataFrame, authority_col: str = "Local Authority") -> pd.DataFrame:
    """
    Remove rows that contain section headers rather than geographic entities.
    
    Args:
        df (pd.DataFrame): DataFrame containing evacuation data
        authority_col (str): Name of the column containing authority names
        
    Returns:
        pd.DataFrame: Filtered DataFrame with non-geographic rows removed
        
    QA Signal:
        Returns count of filtered rows
    """
    if authority_col not in df.columns:
        return df
    
    initial_count = len(df)
    
    # Filter out rows where authority column contains section headers
    mask = ~df[authority_col].str.lower().isin(NON_GEOGRAPHIC_LABELS)
    df_filtered = df[mask].copy()
    
    filtered_count = initial_count - len(df_filtered)
    
    if filtered_count > 0:
        print(f"  → Filtered {filtered_count} non-geographic section header rows")
    
    return df_filtered


def parse_evacuation_dates(df: pd.DataFrame, date_col: str = "Date Evacuation Initiated") -> pd.DataFrame:
    """
    Parse and standardize evacuation date formats.
    
    Creates a new column with standardized datetime format and handles parsing errors.
    
    Args:
        df (pd.DataFrame): DataFrame containing evacuation data
        date_col (str): Name of the column containing date strings
        
    Returns:
        pd.DataFrame: DataFrame with added 'date_initiated_parsed' column
        
    QA Signals:
        - Count of successfully parsed dates
        - Count of unparseable dates
    """
    if date_col not in df.columns:
        print(f"  ⚠️ Warning: Column '{date_col}' not found")
        return df
    
    df = df.copy()
    
    # Parse dates with error handling
    df["date_initiated_parsed"] = pd.to_datetime(
        df[date_col], 
        errors='coerce'
    )
    
    parsed_count = df["date_initiated_parsed"].notna().sum()
    failed_count = len(df) - parsed_count
    
    print(f"  → Parsed {parsed_count}/{len(df)} dates successfully")
    if failed_count > 0:
        print(f"  ⚠️ {failed_count} dates could not be parsed")
    
    return df


def forward_fill_authorities(
    df: pd.DataFrame, 
    authority_col: str = "Local Authority",
    date_col: str = "Date Evacuation Initiated"
) -> pd.DataFrame:
    """
    Forward-fill missing Local Authority values for merged table cells.
    
    Only fills authorities for rows that have valid dates (to avoid 
    propagating across section boundaries).
    
    Args:
        df (pd.DataFrame): DataFrame with potential missing authorities
        authority_col (str): Name of the authority column
        date_col (str): Name of the date column (for validation)
        
    Returns:
        pd.DataFrame: DataFrame with filled authority values
        
    QA Signal:
        Returns count of filled cells
    """
    if authority_col not in df.columns:
        return df
    
    df = df.copy()
    
    # Count empty authorities before filling
    empty_before = df[authority_col].isna().sum() + (df[authority_col] == "").sum()
    
    # Replace empty strings with NA
    df[authority_col] = df[authority_col].replace("", pd.NA)
    
    # Only forward-fill for rows with valid dates
    if date_col in df.columns:
        mask = df[date_col].notna() & (df[date_col] != "")
        df.loc[mask, authority_col] = df.loc[mask, authority_col].ffill()
    else:
        # If no date column, fill all
        df[authority_col] = df[authority_col].ffill()
    
    # Count filled cells
    empty_after = df[authority_col].isna().sum()
    filled_count = empty_before - empty_after
    
    if filled_count > 0:
        print(f"  → Forward-filled {filled_count} missing authority values")
    
    return df


def generate_event_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate unique event identifiers for each evacuation record.
    
    Event ID format: {source}_{authority}_{date}
    
    Args:
        df (pd.DataFrame): DataFrame with source, authority, and date columns
        
    Returns:
        pd.DataFrame: DataFrame with added 'event_id' column
    """
    df = df.copy()
    
    # Build event ID from available columns
    source = df.get("source_name", "unknown").fillna("unknown")
    authority = df.get("Local Authority", "unknown").fillna("unknown").str.replace(" ", "_")
    
    if "date_initiated_parsed" in df.columns:
        date_str = df["date_initiated_parsed"].dt.strftime("%Y%m%d").fillna("unknown")
    else:
        date_str = "unknown"
    
    df["event_id"] = (source + "_" + authority + "_" + date_str).str.lower()
    
    unique_events = df["event_id"].nunique()
    duplicates = len(df) - unique_events
    
    print(f"  → Generated {unique_events} unique event IDs")
    if duplicates > 0:
        print(f"  ⚠️ {duplicates} duplicate event IDs detected")
    
    return df


def clean_wildfire_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Complete cleaning pipeline for wildfire evacuation data.
    
    Performs the following operations in order:
    1. Filter non-geographic section headers
    2. Forward-fill missing Local Authority values
    3. Parse evacuation dates
    4. Normalize authority names
    5. Generate unique event IDs
    
    Args:
        df (pd.DataFrame): Raw wildfire evacuation data
        
    Returns:
        pd.DataFrame: Cleaned and standardized evacuation data
        
    Example:
        >>> raw_df = pd.read_csv("T1_Wildfire_Evacs.csv")
        >>> clean_df = clean_wildfire_data(raw_df)
    """
    print("\n[CLEANING WILDFIRE DATA]")
    
    if df.empty:
        print("  ⚠️ No data to clean")
        return df
    
    # Step 1: Filter non-geographic rows
    print("Step 1: Filtering non-geographic rows...")
    df = filter_non_geographic_rows(df)
    
    # Step 2: Forward-fill authorities (for merged cells)
    print("Step 2: Forward-filling authorities...")
    df = forward_fill_authorities(df)
    
    # Step 3: Parse dates
    print("Step 3: Parsing dates...")
    df = parse_evacuation_dates(df)
    
    # Step 4: Normalize authority names for matching
    print("Step 4: Normalizing authority names...")
    if "Local Authority" in df.columns:
        df["LA_NORM"] = df["Local Authority"].apply(normalize_name)
        print(f"  → Created normalized name column")
    
    # Step 5: Generate event IDs
    print("Step 5: Generating event IDs...")
    df = generate_event_ids(df)
    
    print(f"\n✓ Cleaning complete: {len(df)} records processed")
    
    return df


def clean_census_data(census_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare census data for matching and enrichment.
    
    Extracts and formats demographic data from Statistics Canada census file.
    
    Args:
        census_df (pd.DataFrame): Raw census data from Statistics Canada
        
    Returns:
        pd.DataFrame: Cleaned census lookup table with columns:
            - DGUID: Unique geographic identifier
            - ALT_GEO_CODE: Alternative geographic code
            - GEO_NAME: Official geographic name
            - POP_2021: Total population
            - INDIG_POP_2021: Indigenous population
            - INDIG_DENOM_2021: Denominator for Indigenous identity
            - INDIG_SHARE_2021: Indigenous population share
            - GEO_NAME_NORM: Normalized name for matching
    """
    print("\n[CLEANING CENSUS DATA]")
    
    # Filter to Census Subdivision level only
    csd = census_df[census_df["GEO_LEVEL"] == "Census subdivision"].copy()
    print(f"  → Filtered to {len(csd)} Census Subdivision records")
    
    # Extract total population
    census_pop = csd[csd["CHARACTERISTIC_NAME"] == "Population, 2021"].copy()
    census_pop = census_pop[["DGUID", "ALT_GEO_CODE", "GEO_NAME", "C1_COUNT_TOTAL"]]
    census_pop = census_pop.rename(columns={"C1_COUNT_TOTAL": "POP_2021"})
    
    # Normalize names for matching
    census_pop["GEO_NAME_NORM"] = census_pop["GEO_NAME"].apply(normalize_name)
    
    # Extract Indigenous population
    ind_tot = csd[csd["CHARACTERISTIC_NAME"] == "Indigenous identity"][
        ["DGUID", "C1_COUNT_TOTAL"]
    ].rename(columns={"C1_COUNT_TOTAL": "INDIG_POP_2021"})
    
    # Get denominator for Indigenous identity
    denom = csd[
        csd["CHARACTERISTIC_NAME"] == "Total population in private households by Indigenous identity"
    ][["DGUID", "C1_COUNT_TOTAL"]].rename(
        columns={"C1_COUNT_TOTAL": "INDIG_DENOM_2021"}
    )
    
    # Merge all demographic data
    census_clean = (
        census_pop
        .merge(ind_tot, on="DGUID", how="left")
        .merge(denom, on="DGUID", how="left")
    )
    
    # Calculate Indigenous share
    census_clean["INDIG_SHARE_2021"] = (
        census_clean["INDIG_POP_2021"] / census_clean["INDIG_DENOM_2021"]
    )
    
    print(f"✓ Census cleaning complete: {len(census_clean)} geographic units")
    print(f"  Columns: {', '.join(census_clean.columns)}")
    
    return census_clean


if __name__ == "__main__":
    # Example usage
    print("="*60)
    print("CLEANING MODULE - STANDALONE TEST")
    print("="*60)
    
    # Test wildfire cleaning
    wildfire_df = pd.read_csv("csv files/T1_Wildfire_Evacs.csv")
    wildfire_clean = clean_wildfire_data(wildfire_df)
    wildfire_clean.to_csv("csv files/T1_Wildfire_Evacs_cleaned.csv", index=False)
    
    # Test census cleaning
    census_df = pd.read_csv("csv files/Manitoba_2021_Census.csv")
    census_clean = clean_census_data(census_df)
    census_clean.to_csv("csv files/census_lookup_cleaned.csv", index=False)
    
    print("\n✓ Test complete - cleaned files saved")