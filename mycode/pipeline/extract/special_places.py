"""
DESIGNATED PLACES MODULE (GNBC Integration)

Uses Geographical Names Board of Canada (GNBC) database for designated places.
These are official places not in census subdivisions:
- Lakes and water bodies
- Provincial/national parks
- Unincorporated communities
"""

import pandas as pd
from typing import Optional
from pipeline.extract.gnbc import fetch_gnbc_manitoba, GNBCQASignals


def load_designated_places() -> pd.DataFrame:
    """
    Load designated places from GNBC database.
    
    Returns:
        DataFrame with GNBC designated places data
    """
    print("  → Fetching designated places from GNBC...")
    
    qa_signals = GNBCQASignals()
    
    # Fetch GNBC data for Manitoba
    gnbc_df = fetch_gnbc_manitoba(
        entity_types=['LAKE', 'PARK', 'POPULATED PLACE'],
        qa_signals=qa_signals
    )
    
    # Log QA signals
    for key, value in qa_signals.signals.items():
        print(f"    {key}: {value}")
    
    return gnbc_df


def match_designated_places(
    unmatched_df: pd.DataFrame,
    gnbc_df: pd.DataFrame,
    authority_col: str = "LA_NORM"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Match unmatched authorities against GNBC designated places.
    
    Args:
        unmatched_df: DataFrame of authorities that didn't match census
        gnbc_df: GNBC designated places data
        authority_col: Column name with normalized authority names
        
    Returns:
        Tuple of (matched_df, still_unmatched_df)
    """
    if gnbc_df.empty:
        return pd.DataFrame(), unmatched_df
    
    # Merge on normalized names
    matched = unmatched_df.merge(
        gnbc_df,
        left_on=authority_col,
        right_on='place_name_normalized',
        how='inner'
    )
    
    if matched.empty:
        return pd.DataFrame(), unmatched_df
    
    # Rename/add columns to match census format
    matched = matched.rename(columns={
        'place_name': 'MATCHED_NAME',
        'population': 'POP_2021',
        'indigenous_population': 'INDIG_POP_2021'
    })
    
    # Add census-compatible fields
    matched['DGUID'] = 'GNBC_' + matched['entity_type'].str.upper() + '_' + matched.index.astype(str)
    matched['ALT_GEO_CODE'] = 'DESIGNATED_PLACE'
    matched['GEO_NAME'] = matched['MATCHED_NAME']
    matched['INDIG_DENOM_2021'] = 0
    matched['INDIG_SHARE_2021'] = 0
    matched['match_type'] = 'designated_place_gnbc'
    matched['match_score'] = 100
    
    # Find still unmatched
    matched_authorities = matched[authority_col].unique()
    still_unmatched = unmatched_df[~unmatched_df[authority_col].isin(matched_authorities)]
    
    print(f"  ✓ Matched {len(matched)} records to GNBC designated places")
    if len(matched) > 0:
        print(f"    Breakdown:")
        for entity_type, count in matched['entity_type'].value_counts().items():
            print(f"      - {entity_type}: {count}")
    print(f"  → {len(still_unmatched)} records remain unmatched")
    
    return matched, still_unmatched


def enrich_with_designated_places(
    wildfire_df: pd.DataFrame,
    census_matches: pd.DataFrame
) -> pd.DataFrame:
    """
    Complete enrichment including GNBC designated places.
    
    Process:
    1. Identify records not matched to census
    2. Load GNBC designated places
    3. Match against GNBC data
    4. Combine all matches
    
    Args:
        wildfire_df: Cleaned wildfire data
        census_matches: Records matched to census
        
    Returns:
        Combined enriched dataset
    """
    # Find unmatched records
    if 'DGUID' in census_matches.columns:
        matched_ids = census_matches['event_id'].unique()
        unmatched = wildfire_df[~wildfire_df['event_id'].isin(matched_ids)].copy()
    else:
        unmatched = wildfire_df.copy()
    
    if unmatched.empty:
        print("\n[GNBC DESIGNATED PLACES]")
        print("  → All records matched to census, no designated places needed")
        return census_matches
    
    print("\n[GNBC DESIGNATED PLACES MATCHING]")
    print(f"  → {len(unmatched)} records not in census")
    
    # Load GNBC data
    gnbc_df = load_designated_places()
    
    # Match against designated places
    gnbc_matches, still_unmatched = match_designated_places(
        unmatched,
        gnbc_df
    )
    
    # Combine all matches
    if not gnbc_matches.empty:
        all_matches = pd.concat([census_matches, gnbc_matches], ignore_index=True)
    else:
        all_matches = census_matches
    
    # Report
    total_matched = len(all_matches)
    total_records = len(wildfire_df)
    match_rate = (total_matched / total_records * 100) if total_records > 0 else 0
    
    print(f"\n[ENRICHMENT SUMMARY]")
    print(f"  Census matches: {len(census_matches)}")
    print(f"  GNBC designated places matches: {len(gnbc_matches)}")
    print(f"  Total enriched: {total_matched}/{total_records} ({match_rate:.1f}%)")
    print(f"  Unmatched: {len(still_unmatched)}")
    
    return all_matches


if __name__ == "__main__":
    # Test designated places loading
    print("Testing GNBC designated places module...")
    dp = load_designated_places()
    print(f"\nLoaded {len(dp)} designated places:")
    print(dp[['place_name', 'entity_type', 'generic_category']].head(20).to_string())
