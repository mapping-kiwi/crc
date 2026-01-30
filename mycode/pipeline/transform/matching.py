"""
MATCHING MODULE FOR WILDFIRE EVACUATION ETL PIPELINE

Handles fuzzy matching of local authorities to census geographies and generates
comprehensive match quality reports.

Part of the TRANSFORM stage in the ETL process.

Functions:
    - fuzzy_match_authorities: Match wildfire authorities to census geographies
    - generate_match_report: Create detailed QA report on match quality
    - enrich_with_census: Add census demographics to wildfire data
"""

import pandas as pd
from typing import Dict, List, Tuple
from thefuzz import process
import os


class MatchReport:
    """Track and report on matching quality metrics."""
    
    def __init__(self):
        self.total_authorities = 0
        self.matched_authorities = 0
        self.unmatched_authorities = []
        self.low_confidence_matches = []
        self.match_scores = []
        self.enriched_records = 0
        self.total_records = 0
    
    def add_match(self, authority: str, score: int, dguid: str = None):
        """Record a match attempt."""
        self.total_authorities += 1
        self.match_scores.append(score)
        
        if dguid:
            self.matched_authorities += 1
            if score < 90:  # Flag low confidence matches
                self.low_confidence_matches.append({
                    'authority': authority,
                    'score': score,
                    'dguid': dguid
                })
        else:
            self.unmatched_authorities.append({
                'authority': authority,
                'score': score
            })
    
    def set_enrichment_stats(self, enriched: int, total: int):
        """Set record-level enrichment statistics."""
        self.enriched_records = enriched
        self.total_records = total
    
    def get_match_rate(self) -> float:
        """Calculate percentage of authorities successfully matched."""
        if self.total_authorities == 0:
            return 0.0
        return (self.matched_authorities / self.total_authorities) * 100
    
    def get_enrichment_rate(self) -> float:
        """Calculate percentage of records enriched with census data."""
        if self.total_records == 0:
            return 0.0
        return (self.enriched_records / self.total_records) * 100
    
    def get_score_distribution(self) -> Dict[str, int]:
        """Get distribution of match scores by range."""
        if not self.match_scores:
            return {}
        
        distribution = {
            "Perfect (100)": sum(1 for s in self.match_scores if s == 100),
            "Excellent (90-99)": sum(1 for s in self.match_scores if 90 <= s < 100),
            "Good (80-89)": sum(1 for s in self.match_scores if 80 <= s < 90),
            "Fair (70-79)": sum(1 for s in self.match_scores if 70 <= s < 80),
            "Poor (<70)": sum(1 for s in self.match_scores if s < 70),
        }
        return distribution
    
    def generate_report(self) -> str:
        """Generate formatted match quality report."""
        report = ["\n" + "="*60]
        report.append("MATCH QUALITY REPORT")
        report.append("="*60)
        
        # Overall statistics
        report.append("\n[MATCHING STATISTICS]")
        report.append(f"  Total unique authorities: {self.total_authorities}")
        report.append(f"  Successfully matched: {self.matched_authorities}")
        report.append(f"  Unmatched: {len(self.unmatched_authorities)}")
        report.append(f"  Match rate: {self.get_match_rate():.1f}%")
        
        # Record-level enrichment
        report.append("\n[ENRICHMENT STATISTICS]")
        report.append(f"  Total records: {self.total_records}")
        report.append(f"  Records enriched: {self.enriched_records}")
        report.append(f"  Enrichment rate: {self.get_enrichment_rate():.1f}%")
        
        # Score distribution
        report.append("\n[MATCH SCORE DISTRIBUTION]")
        distribution = self.get_score_distribution()
        for category, count in distribution.items():
            percentage = (count / self.total_authorities * 100) if self.total_authorities > 0 else 0
            report.append(f"  {category}: {count} ({percentage:.1f}%)")
        
        # Low confidence warnings
        if self.low_confidence_matches:
            report.append(f"\n[LOW CONFIDENCE MATCHES] ({len(self.low_confidence_matches)} total)")
            report.append("  Top 5 matches to review:")
            for match in sorted(self.low_confidence_matches, key=lambda x: x['score'])[:5]:
                report.append(f"    - {match['authority']}: score={match['score']}")
        
        # Unmatched authorities
        if self.unmatched_authorities:
            report.append(f"\n[UNMATCHED AUTHORITIES] ({len(self.unmatched_authorities)} total)")
            report.append("  Authorities without census match:")
            for unmatch in self.unmatched_authorities[:10]:  # Show first 10
                report.append(f"    - {unmatch['authority']} (best score: {unmatch['score']})")
            if len(self.unmatched_authorities) > 10:
                report.append(f"    ... and {len(self.unmatched_authorities) - 10} more")
        
        report.append("="*60 + "\n")
        return "\n".join(report)
    
    def save_unmatched(self, output_path: str):
        """Save list of unmatched authorities to CSV."""
        if not self.unmatched_authorities:
            return
        
        df = pd.DataFrame(self.unmatched_authorities)
        df.to_csv(output_path, index=False)
        print(f"  → Saved {len(df)} unmatched authorities to {output_path}")
    
    def save_low_confidence(self, output_path: str):
        """Save low confidence matches for manual review."""
        if not self.low_confidence_matches:
            return
        
        df = pd.DataFrame(self.low_confidence_matches)
        df = df.sort_values('score')
        df.to_csv(output_path, index=False)
        print(f"  → Saved {len(df)} low-confidence matches to {output_path}")


def fuzzy_match_authorities(
    wildfire_df: pd.DataFrame,
    census_df: pd.DataFrame,
    score_cutoff: int = 80,
    authority_col: str = "LA_NORM",
    census_name_col: str = "GEO_NAME_NORM"
) -> Tuple[pd.DataFrame, MatchReport]:
    """
    Match wildfire Local Authorities to census geographies using fuzzy matching.
    
    Uses the Levenshtein distance algorithm to find best matches between
    normalized authority names and census geography names.
    
    Args:
        wildfire_df (pd.DataFrame): Cleaned wildfire data with normalized names
        census_df (pd.DataFrame): Cleaned census data with normalized names
        score_cutoff (int): Minimum match score (0-100) to accept
        authority_col (str): Column name for normalized authority names
        census_name_col (str): Column name for normalized census names
        
    Returns:
        Tuple[pd.DataFrame, MatchReport]: 
            - DataFrame with authority-to-DGUID mappings
            - MatchReport object with quality metrics
    """
    print("\n[FUZZY MATCHING AUTHORITIES]")
    
    # Initialize match report
    report = MatchReport()
    
    # Get unique authorities from wildfire data
    unique_authorities = wildfire_df[[
        "Local Authority", authority_col
    ]].drop_duplicates()
    
    print(f"  Matching {len(unique_authorities)} unique authorities...")
    
    # Prepare census choices for fuzzy matching
    census_choices = census_df[census_name_col].tolist()
    
    matches = []
    
    for _, row in unique_authorities.iterrows():
        la_original = row["Local Authority"]
        la_normalized = row[authority_col]
        
        # Skip empty authorities
        if not la_normalized:
            report.add_match(la_original, 0, None)
            matches.append({
                "Local Authority": la_original,
                "match_name": None,
                "match_score": 0,
                "DGUID": None,
            })
            continue
        
        # Find best fuzzy match
        match_result = process.extractOne(la_normalized, census_choices)
        
        if match_result:
            match_name, score = match_result
        else:
            match_name, score = None, 0
        
        # Accept match if score meets cutoff
        if score >= score_cutoff:
            # Find the DGUID for the matched name
            census_row = census_df[census_df[census_name_col] == match_name].iloc[0]
            dguid = census_row["DGUID"]
            report.add_match(la_original, score, dguid)
            
            matches.append({
                "Local Authority": la_original,
                "match_name": match_name,
                "match_score": score,
                "DGUID": dguid,
            })
        else:
            # No acceptable match found
            report.add_match(la_original, score, None)
            matches.append({
                "Local Authority": la_original,
                "match_name": match_name if match_name else None,
                "match_score": score,
                "DGUID": None,
            })
    
    mapping_df = pd.DataFrame(matches)
    
    print(f"  ✓ Matched {report.matched_authorities}/{report.total_authorities} authorities")
    print(f"  ✓ Match rate: {report.get_match_rate():.1f}%")
    
    return mapping_df, report


def enrich_with_census(
    wildfire_df: pd.DataFrame,
    census_df: pd.DataFrame,
    mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add census demographic data to wildfire evacuation records.
    
    Joins wildfire data with census data using the authority-to-DGUID mapping.
    
    Args:
        wildfire_df (pd.DataFrame): Cleaned wildfire evacuation data
        census_df (pd.DataFrame): Cleaned census demographic data
        mapping_df (pd.DataFrame): Authority-to-DGUID mapping from fuzzy_match_authorities
        
    Returns:
        pd.DataFrame: Wildfire data enriched with census columns:
            - DGUID
            - Census_Pop_2021
            - Census_Indig_Total
            - Census_Indig_Share
    """
    print("\n[ENRICHING WITH CENSUS DATA]")
    
    df = wildfire_df.copy()
    
    # Create lookup dictionary from mapping
    authority_to_dguid = dict(
        zip(mapping_df["Local Authority"], mapping_df["DGUID"])
    )
    
    # Map authorities to DGUIDs
    df["DGUID"] = df["Local Authority"].map(authority_to_dguid)
    
    # Select census columns to join
    census_cols = census_df[[
        "DGUID", 
        "POP_2021", 
        "INDIG_POP_2021", 
        "INDIG_SHARE_2021"
    ]].rename(columns={
        "POP_2021": "Census_Pop_2021",
        "INDIG_POP_2021": "Census_Indig_Total",
        "INDIG_SHARE_2021": "Census_Indig_Share"
    })
    
    # Join census data
    df = df.merge(census_cols, on="DGUID", how="left")
    
    enriched_count = df["DGUID"].notna().sum()
    print(f"  ✓ Enriched {enriched_count}/{len(df)} records with census data")
    print(f"  ✓ Enrichment rate: {(enriched_count/len(df)*100):.1f}%")
    
    return df


def create_matching_pipeline(
    wildfire_df: pd.DataFrame,
    census_df: pd.DataFrame,
    score_cutoff: int = 80,
    output_dir: str = "csv files"
) -> Tuple[pd.DataFrame, MatchReport]:
    """
    Complete matching and enrichment pipeline.
    
    Performs fuzzy matching, generates reports, and enriches wildfire data
    with census demographics.
    
    Args:
        wildfire_df (pd.DataFrame): Cleaned wildfire data
        census_df (pd.DataFrame): Cleaned census data
        score_cutoff (int): Minimum match score threshold
        output_dir (str): Directory for output files
        
    Returns:
        Tuple[pd.DataFrame, MatchReport]:
            - Enriched wildfire DataFrame
            - Match quality report
    """
    print("="*60)
    print("MATCHING PIPELINE - CENSUS ENRICHMENT")
    print("="*60)
    
    # Step 1: Fuzzy match authorities to census geographies
    mapping_df, match_report = fuzzy_match_authorities(
        wildfire_df, 
        census_df, 
        score_cutoff
    )
    
    # Step 2: Enrich wildfire data with census demographics
    enriched_df = enrich_with_census(wildfire_df, census_df, mapping_df)
    
    # Update match report with enrichment stats
    enriched_count = enriched_df["DGUID"].notna().sum()
    match_report.set_enrichment_stats(enriched_count, len(enriched_df))
    
    # Step 3: Save outputs
    print("\n[SAVING MATCH OUTPUTS]")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs("qa_reports", exist_ok=True)
    
    # Save mapping for inspection
    mapping_path = f"{output_dir}/authority_to_dguid_mapping.csv"
    mapping_df.to_csv(mapping_path, index=False)
    print(f"  ✓ Saved mapping to {mapping_path}")
    
    # Save unmatched authorities
    unmatched_path = f"{output_dir}/unmatched_authorities.csv"
    match_report.save_unmatched(unmatched_path)
    
    # Save low confidence matches
    low_conf_path = f"{output_dir}/low_confidence_matches.csv"
    match_report.save_low_confidence(low_conf_path)
    
    # Generate and display report
    print(match_report.generate_report())
    
    # Save report to file
    report_path = "qa_reports/match_quality_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(match_report.generate_report())
    print(f"✓ Match quality report saved to {report_path}\n")
    
    return enriched_df, match_report

def create_matching_pipeline_with_designated_places(
    wildfire_df: pd.DataFrame,
    census_df: pd.DataFrame,
    score_cutoff: int = 80,
    output_dir: str = "csv files"
) -> tuple[pd.DataFrame, 'MatchReport']:
    """Enhanced matching pipeline including GNBC designated places."""
    from pipeline.extract.special_places import enrich_with_designated_places
    
    print("\n[ENHANCED MATCHING WITH GNBC DESIGNATED PLACES]")
    
    # Step 1: Regular census matching
    enriched_census, report = create_matching_pipeline(
        wildfire_df,
        census_df,
        score_cutoff,
        output_dir
    )
    
    # Step 2: Add GNBC designated places for unmatched
    final_enriched = enrich_with_designated_places(
        wildfire_df,
        enriched_census
    )
    
    return final_enriched, report

if __name__ == "__main__":
    # Example usage - complete matching pipeline
    print("="*60)
    print("MATCHING MODULE - STANDALONE TEST")
    print("="*60)
    
    # Load cleaned data (assuming cleaning.py was run first)
    wildfire_df = pd.read_csv("csv files/T1_Wildfire_Evacs_cleaned.csv")
    census_df = pd.read_csv("csv files/census_lookup_cleaned.csv")
    
    # Run matching pipeline
    enriched_df, report = create_matching_pipeline(
        wildfire_df,
        census_df,
        score_cutoff=80
    )
    
    # Save enriched output
    output_path = "csv files/T1_Wildfire_Evacs_enriched.csv"
    enriched_df.to_csv(output_path, index=False)
    print(f"✓ Saved enriched data to {output_path}")
    
    print("\n" + "="*60)
    print("MATCHING TEST COMPLETE")
    print("="*60)