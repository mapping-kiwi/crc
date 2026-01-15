# census_data_prep.py
"""
DATA PREPARATION MODULE FOR CENSUS ENRICHMENT PIPELINE
Handles loading and cleaning of census and wildfire evacuation data.
"""

import pandas as pd
from typing import Optional


def normalize_name(s: str) -> str:
    """
    Clean place names for better matching.
    Remove "Town of", "City of", "RM of", etc., lowercase, trim whitespace.
    
    Args:
        s (str): Place name to normalize
        
    Returns:
        str: Normalized place name
    """
    if not isinstance(s, str):
        return ""
    
    s = s.lower().strip()
    
    # Common prefixes to strip
    for pref in ["town of ", "city of ", "rm of ", "r.m. of ", "rural municipality of "]:
        if s.startswith(pref):
            s = s[len(pref):]
    
    return s.replace("  ", " ")  # Handle double spaces


def load_census_data(census_path: str) -> pd.DataFrame:
    """
    Load raw census CSV data.
    
    Args:
        census_path (str): Path to census CSV file
        
    Returns:
        pd.DataFrame: Raw census data
    """
    print(f"Loading census data from {census_path}...")
    return pd.read_csv(census_path)


def build_census_lookup(census_path: str) -> pd.DataFrame:
    """
    Reads Stats Canada 2021 Census CSV file and builds a demographic lookup DataFrame.
    
    Returns a DataFrame with columns:
    - DGUID
    - ALT_GEO_CODE
    - GEO_NAME
    - POP_2021
    - INDIG_POP_2021
    - INDIG_DENOM_2021
    - INDIG_SHARE_2021
    - GEO_NAME_NORM (normalized name for matching)
    
    Args:
        census_path (str): Path to the CSV census data file.
        
    Returns:
        pd.DataFrame: DataFrame with demographic data for census subdivisions.
    """
    census = load_census_data(census_path)
    
    # Filter to census subdivision level
    csd = census[census["GEO_LEVEL"] == "Census subdivision"].copy()
    
    # Get total population for each CSD
    census_pop = csd[csd["CHARACTERISTIC_NAME"] == "Population, 2021"].copy()
    census_pop = census_pop[["DGUID", "ALT_GEO_CODE", "GEO_NAME", "C1_COUNT_TOTAL"]]
    census_pop = census_pop.rename(columns={"C1_COUNT_TOTAL": "POP_2021"})
    census_pop["GEO_NAME_NORM"] = census_pop["GEO_NAME"].apply(normalize_name)
    
    # Indigenous population totals
    ind_tot = csd[csd["CHARACTERISTIC_NAME"] == "Indigenous identity"][
        ["DGUID", "C1_COUNT_TOTAL"]
    ].rename(columns={"C1_COUNT_TOTAL": "INDIG_POP_2021"})
    
    # Get total population denominator for Indigenous identity
    denom = csd[
        csd["CHARACTERISTIC_NAME"] == "Total population in private households by Indigenous identity"
    ][["DGUID", "C1_COUNT_TOTAL"]].rename(
        columns={"C1_COUNT_TOTAL": "INDIG_DENOM_2021"}
    )
    
    # Merge all together
    demo = (
        census_pop
        .merge(ind_tot, on="DGUID", how="left")
        .merge(denom, on="DGUID", how="left")
    )
    
    demo["INDIG_SHARE_2021"] = (
        demo["INDIG_POP_2021"] / demo["INDIG_DENOM_2021"]
    )
    
    return demo


def load_wildfire_data(wildfire_path: str) -> pd.DataFrame:
    """
    Load and clean wildfire evacuation data.
    
    Args:
        wildfire_path (str): Path to wildfire CSV file
        
    Returns:
        pd.DataFrame: Cleaned wildfire data with normalized Local Authority column
    """
    print(f"Loading wildfire data from {wildfire_path}...")
    wildfire = pd.read_csv(wildfire_path)
    
    # Forward-fill blank Local Authority values (common in government tables)
    if "Local Authority" in wildfire.columns:
        wildfire["Local Authority"] = (
            wildfire["Local Authority"].replace("", pd.NA).ffill()
        )
        wildfire["LA_NORM"] = wildfire["Local Authority"].apply(normalize_name)
    
    return wildfire


def prepare_authority_list(wildfire_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique local authorities from wildfire data and prepare for matching.
    
    Args:
        wildfire_df (pd.DataFrame): Wildfire evacuation data
        
    Returns:
        pd.DataFrame: DataFrame with unique Local Authorities and normalized names
    """
    authorities = pd.Series(
        wildfire_df["Local Authority"].unique(), 
        name="Local Authority"
    ).to_frame()
    
    authorities["LA_NORM"] = authorities["Local Authority"].apply(normalize_name)
    
    return authorities


def save_census_lookup(census_demo: pd.DataFrame, output_path: str) -> None:
    """
    Save prepared census lookup table to CSV.
    
    Args:
        census_demo (pd.DataFrame): Census demographic lookup table
        output_path (str): Path to save CSV file
    """
    census_demo.to_csv(output_path, index=False)
    print(f"Saved census lookup to {output_path}")


if __name__ == "__main__":
    # Example usage: prepare census lookup table
    census_path = "csv files/Manitoba_2021_Census.csv"
    output_path = "csv files/census_lookup_prepared.csv"
    
    census_demo = build_census_lookup(census_path)
    save_census_lookup(census_demo, output_path)
    
    print(f"\nCensus lookup prepared with {len(census_demo)} records")
    print(f"Columns: {list(census_demo.columns)}")