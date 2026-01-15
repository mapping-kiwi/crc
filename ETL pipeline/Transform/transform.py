# t1_census_pipeline.py

"""
CENSUS ENRICHMENT PIPELINE FOR TIER 1 WILDFIRE EVACUATION DATA
This python code matches Local Authority names from wildfire evacuation data
to Census DGUIDs, and fetches census demographic data for those areas.

"""

# Python imports
import json # Read/write JSON data (used for StatCan API responses)
from typing import Tuple, Dict, Any
import pandas as pd
import requests
from thefuzz import process # Fuzzy string matching for name matching (ie "Garden Hill" -> "Garden Hill First Nation")

def normalize_name(s: str) -> str:
    
    """
    Clean place names for better matching.
    Remove "Town of", "City of", "RM of", etc., lowercase, trim whitespace.
    """
    
    if not isinstance(s, str): # check if input is actually a string
        return "" # empty if not a string
    
    s = s.lower().strip() # convert to lowercase, remove leading/trailing whitespace
    
    # common prefixes to strip
    for pref in ["town of ", "city of ", "rm of ", "r.m. of ", "rural municipality of "]:
        if s.startswith(pref):
            s = s[len(pref):]
            
    return s.replace("  ", " ") # double space handling

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
    
    # Read StatsCan 2021 Census CSV
    print(f"Loading census data from {census_path}...")
    census = pd.read_csv(census_path)

    # Filter to census subdivision level and 2021 population
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
        csd["CHARACTERISTIC_NAME"]
        == "Total population in private households by Indigenous identity"
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


def auto_match_local_authorities(
    wildfire_df: pd.DataFrame, 
    census_demo: pd.DataFrame, 
    score_cutoff: int = 80
) -> pd.DataFrame:
    """
    Automatically match Local Authority names 
    from wildfire_df to Census communities using 
    fuzzy matching
    
        Args:
        wildfire_df: Evacuation data with "Local Authority" column
        census_demo: Census lookup from build_census_lookup()
        score_cutoff: Minimum match confidence (0-100)
    
    Returns:
        DataFrame showing matches + census data
    """
    
    # Get unique LAs
    authorities = pd.Series(
        wildfire_df["Local Authority"].unique(), name="Local Authority"
    ).to_frame()
    authorities["LA_NORM"] = authorities["Local Authority"].apply(normalize_name)

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

        # find census row for best match
        c_row = census_demo[census_demo["GEO_NAME_NORM"] == match].iloc[0]
        matches.append({
            "Local Authority": la,
            "match_score": score,
            "DGUID": c_row["DGUID"],
        })

    mapping_auto = pd.DataFrame(matches)
    result = mapping_auto.merge(census_demo, on="DGUID", how="left")
    print(f"Auto-matched {len(result)} Local Authorities with cutoff {score_cutoff}")
    return result


# MAIN ENRICHMENT FUNCTION
def enrich_with_census(
    wildfire_df: pd.DataFrame,
    census_path: str,
    score_cutoff: int = 80, # minimum fuzzy match score
    local_to_dguid: Dict[str, str] | None = None,
) -> pd.DataFrame:
    
    """
    Add DGUID, Census_Pop_2021, Census_Indig_Total columns to wildfire_df.
    
    Load census data, authomatically match Local Authority names to DGUIDs,
    and fetch census demographic data.
    
        Args:
            wildfire_df (pd.DataFrame): DataFrame with "Local Authority" column.
            local_to_dguid (Dict[str, str], optional):  
                Predefined mapping of Local Authority names to DGUIDs.
                If None, uses load_local_to_dguid_mapping().
        Returns:
            pd.DataFrame: Enriched DataFrame with census demographic columns.

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
    matched_mapping = {
        normalize_name(row["Local Authority"]): row["DGUID"]
        for _, row in mapping_df.iterrows() if pd.notna(row["DGUID"])
    }
        
        # Clean and match Local Authority column in main dataframe
    if "Local Authority" in wildfire_df.columns:
            # Forward-fill blank Local Authority values (common in government tables)
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
            c_row = census_demo[census_demo["DGUID"] == dguid].iloc[0] if len(census_demo[census_demo["DGUID"] == dguid]) > 0 else None
            if c_row is None:
                 return pd.Series({
                        "Census_Pop_2021": None, 
                        "Census_Indig_Total": None,
                        "Census_Indig_Share": None
                })
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
    print(f"  Download link: https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/dt-td/Rp-eng.cfm?LANG=E&TABID=0")
        
    return wildfire_df

wildfire = pd.read_csv("csv files/T1_Wildfire_Evacs.csv")
census = pd.read_csv("csv files/Manitoba_2021_Census.csv")

# Filter census to 2021 population rows at CSD level
census_pop = census[
    (census["GEO_LEVEL"] == "Census subdivision") &
    (census["CHARACTERISTIC_NAME"] == "Population, 2021")
].copy()

census_pop = census_pop[["DGUID", "ALT_GEO_CODE", "GEO_NAME", "C1_COUNT_TOTAL"]]
census_pop = census_pop.rename(columns={"C1_COUNT_TOTAL": "POP_2021"})

def normalize_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    # strip common prefixes
    for pref in ["town of ", "city of ", "rm of ", "r.m. of ", "rural municipality of "]:
        if s.startswith(pref):
            s = s[len(pref):]
    # simple cleanups
    return s.replace("  ", " ")

# Precompute normalized census names
census_pop["GEO_NAME_NORM"] = census_pop["GEO_NAME"].apply(normalize_name)

# Unique local authorities needing matches
authorities = pd.Series(wildfire["Local Authority"].unique(), name="Local Authority")
authorities = authorities.to_frame()
authorities["LA_NORM"] = authorities["Local Authority"].apply(normalize_name)

# List of reference names to match against
choices = census_pop["GEO_NAME_NORM"].tolist()

def best_census_match(name_norm: str, choices, census_df, score_cutoff=80):
    if not name_norm:
        return None, 0
    match, score = process.extractOne(name_norm, choices)
    if score < score_cutoff:
        return None, score
    row = census_df[census_df["GEO_NAME_NORM"] == match].iloc[0]
    return row, score

matches = []
for _, row in authorities.iterrows():
    census_row, score = best_census_match(row["LA_NORM"], choices, census_pop)
    if census_row is None:
        matches.append({
            "Local Authority": row["Local Authority"],
            "match_score": score,
            "DGUID": None,
            "ALT_GEO_CODE": None,
            "GEO_NAME": None,
            "POP_2021": None,
        })
    else:
        matches.append({
            "Local Authority": row["Local Authority"],
            "match_score": score,
            "DGUID": census_row["DGUID"],
            "ALT_GEO_CODE": census_row["ALT_GEO_CODE"],
            "GEO_NAME": census_row["GEO_NAME"],
            "POP_2021": census_row["POP_2021"],
        })

mapping_auto = pd.DataFrame(matches)


# Inspect low scores by hand (e.g., < 90)
print(mapping_auto.sort_values("match_score").head(10))

# Optionally fix specific rows and save for reuse
mapping_auto.to_csv("csv files/local_to_dguid_mapping_auto.csv", index=False)

# Join population into wildfire table
wildfire_with_pop = wildfire.merge(
    mapping_auto[["Local Authority", "DGUID", "ALT_GEO_CODE", "POP_2021"]],
    on="Local Authority",
    how="left"
)

wildfire_with_pop.to_csv("csv files/T1_Wildfire_Evacs_with_pop.csv", index=False)
