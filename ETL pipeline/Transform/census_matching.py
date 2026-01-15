# census_matching.py
"""
MATCHING AND ENRICHMENT MODULE FOR CENSUS ENRICHMENT PIPELINE
Handles fuzzy matching of local authorities to census geographies and enrichment.
"""

import pandas as pd
from typing import Dict, Optional
from thefuzz import process

# Import from the data prep module
from census_data_prep import (
    normalize_name, 
    build_census_lookup, 
    load_wildfire_data,
    prepare_authority_list
)


def auto_match_local_authorities(
    wildfire_df: pd.DataFrame,
    census_demo: pd.DataFrame,
    score_cutoff: int = 80
) -> pd.DataFrame:
    """
    Automatically match Local Authority names from wildfire_df to Census communities
    using fuzzy matching.
    
    Args:
        wildfire_df (pd.DataFrame): Evacuation data with "Local Authority" column
        census_demo (pd.DataFrame): Census lookup from build_census_lookup()
        score_cutoff (int): Minimum match confidence (0-100)
        
    Returns:
        pd.DataFrame: DataFrame showing matches + census data
    """
    # Get unique LAs
    authorities = prepare_authority_list(wildfire_df)
    
    # Prep census names for fuzzy matching
    choices = census_demo["GEO_NAME_NORM"].tolist()
    
    matches = []
    for _, row in authorities.iterrows():
        la = row["Local Authority"]
        key = row["LA_NORM"]
        
        if not key:
            matches.append({
                "Local Authority": la,
                "match_score": 0,
                "DGUID": None,
            })
            continue
        
        match, score = process.extractOne(key, choices)
        
        if score < score_cutoff:
            matches.append({
                "Local Authority": la,
                "match_score": score,
                "DGUID": None,
            })
            continue
        
        # Find census row for best match
        c_row = census_demo[census_demo["GEO_NAME_NORM"] == match].iloc[0]
        matches.append({
            "Local Authority": la,
            "match_score": score,
            "DGUID": c_row["DGUID"],
        })
    
    mapping_auto = pd.DataFrame(matches)
    result = mapping_auto.merge(census_demo, on="DGUID", how="left")
    
    print(f"Auto-matched {len(result)} Local Authorities with cutoff {score_cutoff}")
    matched = result["DGUID"].notna().sum()
    print(f"  - Successfully matched: {matched}")
    print(f"  - Unmatched: {len(result) - matched}")
    
    return result


def create_mapping_dict(mapping_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create a dictionary mapping normalized Local Authority names to DGUIDs.
    
    Args:
        mapping_df (pd.DataFrame): DataFrame from auto_match_local_authorities()
        
    Returns:
        Dict[str, str]: Dictionary mapping normalized names to DGUIDs
    """
    return {
        normalize_name(row["Local Authority"]): row["DGUID"]
        for _, row in mapping_df.iterrows()
        if pd.notna(row["DGUID"])
    }


def enrich_with_census(
    wildfire_df: pd.DataFrame,
    census_path: str,
    score_cutoff: int = 80,
    local_to_dguid: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Add DGUID, Census_Pop_2021, Census_Indig_Total columns to wildfire_df.
    Load census data, automatically match Local Authority names to DGUIDs,
    and fetch census demographic data.
    
    Args:
        wildfire_df (pd.DataFrame): DataFrame with "Local Authority" column
        census_path (str): Path to census CSV file
        score_cutoff (int): Minimum fuzzy match score (0-100)
        local_to_dguid (Dict[str, str], optional): Predefined mapping of Local 
            Authority names to DGUIDs. If None, uses auto-matching.
        
    Returns:
        pd.DataFrame: Enriched DataFrame with census demographic columns
    """
    wildfire_df = wildfire_df.copy()
    
    # Load census lookup table (offline)
    print("1. Loading census data...")
    census_demo = build_census_lookup(census_path)
    
    # Auto-match Local Authorities to census geographies
    print("2. Matching Local Authorities...")
    mapping_df = auto_match_local_authorities(wildfire_df, census_demo, score_cutoff)
    
    # Create lookup dict for fast matching
    print("3. Creating lookup dictionary...")
    matched_mapping = create_mapping_dict(mapping_df)
    
    # Clean and match Local Authority column in main dataframe
    if "Local Authority" in wildfire_df.columns:
        # Forward-fill blank Local Authority values
        wildfire_df["Local Authority"] = (
            wildfire_df["Local Authority"].replace("", pd.NA).ffill()
        )
        
        # Normalize and map to DGUID
        wildfire_df["LA_NORM"] = wildfire_df["Local Authority"].apply(normalize_name)
        wildfire_df["DGUID"] = wildfire_df["LA_NORM"].map(matched_mapping)
        
        # Add census population columns (direct lookup from census_demo)
        def add_census_row(dguid):
            if pd.isna(dguid):
                return pd.Series({
                    "Census_Pop_2021": None,
                    "Census_Indig_Total": None,
                    "Census_Indig_Share": None
                })
            
            matches = census_demo[census_demo["DGUID"] == dguid]
            if len(matches) == 0:
                return pd.Series({
                    "Census_Pop_2021": None,
                    "Census_Indig_Total": None,
                    "Census_Indig_Share": None
                })
            
            c_row = matches.iloc[0]
            return pd.Series({
                "Census_Pop_2021": c_row["POP_2021"],
                "Census_Indig_Total": c_row["INDIG_POP_2021"],
                "Census_Indig_Share": c_row["INDIG_SHARE_2021"]
            })
        
        print("4. Adding census columns...")
        census_cols = wildfire_df["DGUID"].apply(add_census_row)
        wildfire_df = pd.concat([wildfire_df, census_cols], axis=1)
    else:
        # No Local Authority column - add empty census columns
        for col in ["DGUID", "Census_Pop_2021", "Census_Indig_Total", "Census_Indig_Share"]:
            wildfire_df[col] = pd.NA
    
    matched_count = wildfire_df['DGUID'].notna().sum()
    print(f"✓ Enriched {matched_count} / {len(wildfire_df)} records with census data")
    
    return wildfire_df


def save_mapping(mapping_df: pd.DataFrame, output_path: str) -> None:
    """
    Save the Local Authority to DGUID mapping to CSV for inspection/reuse.
    
    Args:
        mapping_df (pd.DataFrame): Mapping from auto_match_local_authorities()
        output_path (str): Path to save CSV file
    """
    mapping_df.to_csv(output_path, index=False)
    print(f"Saved mapping to {output_path}")


if __name__ == "__main__":
    # Example usage: match and enrich wildfire data
    wildfire_path = "csv files/T1_Wildfire_Evacs.csv"
    census_path = "csv files/Manitoba_2021_Census.csv"
    
    # Load wildfire data
    wildfire = load_wildfire_data(wildfire_path)
    
    # Enrich with census data
    wildfire_enriched = enrich_with_census(
        wildfire, 
        census_path, 
        score_cutoff=80
    )
    
    # Save results
    output_path = "csv files/T1_Wildfire_Evacs_enriched.csv"
    wildfire_enriched.to_csv(output_path, index=False)
    print(f"\nSaved enriched data to {output_path}")
    
    # Optionally save the mapping for review
    census_demo = build_census_lookup(census_path)
    mapping = auto_match_local_authorities(wildfire, census_demo, score_cutoff=80)
    save_mapping(mapping, "csv files/local_to_dguid_mapping.csv")