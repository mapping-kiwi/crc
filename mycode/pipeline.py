"""
WILDFIRE EVACUATION DATA PIPELINE
Manitoba Tier 1 Government Sources

Complete ETL pipeline orchestrating:
    EXTRACT → Scrape wildfire evacuation data from government websites
    TRANSFORM → Clean, normalize, and enrich with census demographics
    LOAD → Export all outputs, QA reports, and summaries

USAGE:
    python pipeline.py                    # Run full pipeline
    python pipeline.py --cutoff 85        # Run with custom match threshold
    python pipeline.py --skip-scraping    # Use existing scraped data

OUTPUTS:
    - csv files/T1_Wildfire_Evacs_Enriched.csv (latest)
    - csv files/T1_Wildfire_Evacs_Enriched_YYYYMMDD_HHMMSS.csv (versioned)
    - qa_reports/QA_Pipeline_YYYYMMDD_HHMMSS.txt (quality report)
    - csv files/unmatched_authorities.csv (requires manual review)
"""

import argparse
import sys
import pandas as pd
from datetime import datetime, UTC

# ETL Modules - organized by pipeline stage
from pipeline.extract.t1_manitoba import scrape_wildfire_data, T1_URLS, QASignals
from pipeline.transform.cleaning import clean_wildfire_data, clean_census_data
from pipeline.transform.matching import create_matching_pipeline, MatchReport
from io_paths import IOPaths
from pipeline.load.export import ExportManager
from statscan_api import fetch_manitoba_census_2021


class PipelineOrchestrator:
    """Orchestrate the complete ETL pipeline with QA tracking."""
    
    def __init__(self, match_cutoff: int = 80, skip_scraping: bool = False):
        """
        Initialize pipeline orchestrator.
        
        Args:
            match_cutoff (int): Minimum fuzzy match score (0-100)
            skip_scraping (bool): If True, use existing scraped data
        """
        self.match_cutoff = match_cutoff
        self.skip_scraping = skip_scraping
        
        # Initialize paths and managers
        self.paths = IOPaths()
        self.export_manager = ExportManager(self.paths)
        
        # QA tracking
        self.scraping_qa = QASignals()
        self.match_report = None
        
        # Pipeline start time
        self.start_time = datetime.now(UTC)
    
    def print_header(self, stage: str):
        """Print stage header."""
        print("\n" + "="*60)
        print(f"STAGE: {stage}")
        print("="*60)
    
    def extract(self) -> pd.DataFrame:
        """
        EXTRACT: Scrape wildfire evacuation data from government websites.
        
        Returns:
            pd.DataFrame: Raw scraped wildfire data
        """
        self.print_header("EXTRACT - Scraping Wildfire Data")
        
        if self.skip_scraping:
            print("\n⚠️ Skipping scraping - loading existing data...")
            if not pd.io.common.file_exists(self.paths.wildfire_input):
                print(f"❌ ERROR: File not found: {self.paths.wildfire_input}")
                print("Please run without --skip-scraping to scrape fresh data.")
                sys.exit(1)
            
            wildfire_df = pd.read_csv(self.paths.wildfire_input)
            print(f"✓ Loaded {len(wildfire_df)} records from {self.paths.wildfire_input}")
            return wildfire_df
        
        # Scrape fresh data
        print(f"\nScraping {len(T1_URLS)} source(s)...")
        wildfire_df = scrape_wildfire_data(T1_URLS, self.scraping_qa)
        
        if wildfire_df.empty:
            print("\n❌ ERROR: No data scraped. Check:")
            print("  1. Internet connection")
            print("  2. Government website structure hasn't changed")
            print("  3. URL is still valid")
            sys.exit(1)
        
        # Export scraped data
        self.export_manager.export_scraped_wildfire(wildfire_df)
        
        print(f"\n✓ EXTRACT complete: {len(wildfire_df)} records scraped")
        return wildfire_df
    


    def transform_clean(self, wildfire_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        TRANSFORM (Part 1): Clean and normalize data.
        """
        self.print_header("TRANSFORM - Cleaning & Normalizing Data")
        
        # Clean wildfire data
        print("\n[1/2] Cleaning wildfire evacuation data...")
        wildfire_clean = clean_wildfire_data(wildfire_df)
        
        # Clean census data - FETCH FROM API
        print("\n[2/2] Fetching census data from Statistics Canada API...")
        from statscan_api import fetch_manitoba_census_2021, CensusQASignals
        
        census_qa = CensusQASignals()
        census_raw = fetch_manitoba_census_2021(census_qa)
        
        # Log QA signals
        for key, value in census_qa.signals.items():
            print(f"  {key}: {value}")
        
        census_clean = clean_census_data(census_raw)
        
        # Export cleaned datasets
        self.export_manager.export_cleaned_wildfire(wildfire_clean)
        self.export_manager.export_cleaned_census(census_clean)
        
        print(f"\n✓ TRANSFORM (Cleaning) complete")
        return wildfire_clean, census_clean
    
    def transform_enrich(
        self, 
        wildfire_clean: pd.DataFrame, 
        census_clean: pd.DataFrame
    ) -> pd.DataFrame:
        """
        TRANSFORM (Part 2): Match and enrich with census data.
        
        Args:
            wildfire_clean (pd.DataFrame): Cleaned wildfire data
            census_clean (pd.DataFrame): Cleaned census data
            
        Returns:
            pd.DataFrame: Enriched wildfire data with census demographics
        """
        self.print_header("TRANSFORM - Matching & Enriching with Census Data")
        
        print(f"\nFuzzy matching with cutoff score: {self.match_cutoff}")
        
        # Run matching pipeline
        enriched_df, match_report = create_matching_pipeline(
            wildfire_clean,
            census_clean,
            score_cutoff=self.match_cutoff,
            output_dir=self.paths.csv_dir
        )
        
        # Store match report for later
        self.match_report = match_report
        
        # Check match quality
        match_rate = match_report.get_match_rate()
        enrichment_rate = match_report.get_enrichment_rate()
        
        if match_rate < 70:
            print(f"\n⚠️ WARNING: Low match rate ({match_rate:.1f}%)")
            print("Consider:")
            print("  1. Lowering --cutoff threshold")
            print("  2. Reviewing unmatched_authorities.csv for manual mapping")
        
        if enrichment_rate < 80:
            print(f"\n⚠️ WARNING: Low enrichment rate ({enrichment_rate:.1f}%)")
            print("Check low_confidence_matches.csv for quality issues")
        
        print(f"\n✓ TRANSFORM (Enrichment) complete: {enrichment_rate:.1f}% records enriched")
        return enriched_df
    
    def load(self, enriched_df: pd.DataFrame):
        """
        LOAD: Export all final outputs and generate reports.
        
        Args:
            enriched_df (pd.DataFrame): Final enriched wildfire data
        """
        self.print_header("LOAD - Exporting Outputs & Reports")
        
        # Export enriched data (both latest and versioned)
        self.export_manager.export_enriched_wildfire(enriched_df)
        
        # Generate pipeline summary report
        pipeline_report = self.generate_pipeline_summary(enriched_df)
        
        # Export QA reports
        print("\n[Saving QA Reports]")
        self.export_manager.export_qa_report(self.scraping_qa.report(), "scraping")
        self.export_manager.export_qa_report(self.match_report.generate_report(), "matching")
        self.export_manager.export_qa_report(pipeline_report, "pipeline")
        
        # Generate export summary
        self.export_manager.save_export_summary()
        
        print(f"\n✓ LOAD complete: All outputs saved")
    
    def generate_pipeline_summary(self, enriched_df: pd.DataFrame) -> str:
        """
        Generate comprehensive pipeline summary report.
        
        Args:
            enriched_df (pd.DataFrame): Final enriched data
            
        Returns:
            str: Formatted pipeline summary
        """
        end_time = datetime.now(UTC)
        duration = (end_time - self.start_time).total_seconds()
        
        summary = ["="*60]
        summary.append("WILDFIRE EVACUATION PIPELINE - SUMMARY REPORT")
        summary.append("="*60)
        summary.append(f"Run Timestamp: {self.paths.run_timestamp}")
        summary.append(f"Duration: {duration:.2f} seconds")
        summary.append(f"Match Cutoff: {self.match_cutoff}")
        
        # Extract stage summary
        summary.append("\n[EXTRACT STAGE]")
        summary.append(f"  Records scraped: {self.scraping_qa.signals.get('records_scraped', 0)}")
        summary.append(f"  Non-geographic rows filtered: {self.scraping_qa.signals.get('non_geographic_rows_filtered', 0)}")
        
        # Transform stage summary
        summary.append("\n[TRANSFORM STAGE]")
        summary.append(f"  Unique authorities: {self.scraping_qa.signals.get('unique_authorities', 0)}")
        summary.append(f"  Records with dates: {self.scraping_qa.signals.get('records_with_dates', 0)}")
        summary.append(f"  Forward-filled authorities: {self.scraping_qa.signals.get('forward_filled_authorities', 0)}")
        
        if self.match_report:
            summary.append(f"  Match rate: {self.match_report.get_match_rate():.1f}%")
            summary.append(f"  Enrichment rate: {self.match_report.get_enrichment_rate():.1f}%")
        
        # Load stage summary
        summary.append("\n[LOAD STAGE]")
        summary.append(f"  Final enriched records: {len(enriched_df)}")
        
        enriched_count = enriched_df['DGUID'].notna().sum()
        summary.append(f"  Records with census data: {enriched_count}")
        
        if 'Census_Pop_2021' in enriched_df.columns:
            total_pop = enriched_df['Census_Pop_2021'].sum()
            summary.append(f"  Total population affected: {total_pop:,.0f}")
        
        if 'Census_Indig_Total' in enriched_df.columns:
            total_indig = enriched_df['Census_Indig_Total'].sum()
            summary.append(f"  Indigenous population affected: {total_indig:,.0f}")
        
        # Data quality warnings
        summary.append("\n[DATA QUALITY]")
        
        if self.match_report and self.match_report.unmatched_authorities:
            summary.append(f"  ⚠️ {len(self.match_report.unmatched_authorities)} unmatched authorities")
            summary.append("     → Review: csv files/unmatched_authorities.csv")
        
        if self.match_report and self.match_report.low_confidence_matches:
            summary.append(f"  ⚠️ {len(self.match_report.low_confidence_matches)} low-confidence matches")
            summary.append("     → Review: csv files/low_confidence_matches.csv")
        
        # Output files
        summary.append("\n[KEY OUTPUT FILES]")
        summary.append(f"  Main Output: {self.paths.enriched_wildfire_latest}")
        summary.append(f"  Versioned: {self.paths.enriched_wildfire_versioned}")
        summary.append(f"  QA Report: {self.paths.qa_report_pipeline}")
        
        summary.append("\n" + "="*60)
        summary.append("PIPELINE COMPLETE")
        summary.append("="*60)
        
        return "\n".join(summary)
    
    def run(self):
        """Execute the complete ETL pipeline."""
        print("\n" + "="*60)
        print("WILDFIRE EVACUATION DATA PIPELINE")
        print("Manitoba Tier 1 Government Sources")
        print("="*60)
        print(f"Run Timestamp: {self.paths.run_timestamp}")
        print(f"Match Cutoff: {self.match_cutoff}")
        print(f"Skip Scraping: {self.skip_scraping}")
        
        try:
            # EXTRACT
            wildfire_raw = self.extract()
            
            # TRANSFORM (Clean)
            wildfire_clean, census_clean = self.transform_clean(wildfire_raw)
            
            # TRANSFORM (Enrich)
            wildfire_enriched = self.transform_enrich(wildfire_clean, census_clean)
            
            # LOAD
            self.load(wildfire_enriched)
            
            # Final summary
            print("\n" + "="*60)
            print("✓ PIPELINE COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"\nMain output file:")
            print(f"  → {self.paths.enriched_wildfire_latest}")
            print(f"\nFor detailed results, see:")
            print(f"  → {self.paths.qa_report_pipeline}")
            
            # Display match quality if concerning
            if self.match_report:
                match_rate = self.match_report.get_match_rate()
                if match_rate < 80:
                    print(f"\n⚠️ Match rate is {match_rate:.1f}% - review unmatched authorities")
            
            print("\n")
            
        except KeyboardInterrupt:
            print("\n\n⚠️ Pipeline interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n\n❌ PIPELINE FAILED")
            print(f"Error: {str(e)}")
            print("\nFor debugging, check:")
            print("  1. Internet connection (for scraping)")
            print("  2. Input file exists: csv files/Manitoba_2021_Census.csv")
            print("  3. Government website structure hasn't changed")
            raise


def main():
    """Main entry point with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Manitoba Wildfire Evacuation Data ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py                    # Run full pipeline
  python pipeline.py --cutoff 85        # Use 85% match threshold
  python pipeline.py --skip-scraping    # Use existing scraped data
  
Output Files:
  - csv files/T1_Wildfire_Evacs_Enriched.csv (main output)
  - qa_reports/QA_Pipeline_YYYYMMDD_HHMMSS.txt (quality report)
  - csv files/unmatched_authorities.csv (requires review)
        """
    )
    
    parser.add_argument(
        "--cutoff",
        type=int,
        default=80,
        metavar="N",
        help="Minimum fuzzy match score (0-100, default: 80)"
    )
    
    parser.add_argument(
        "--skip-scraping",
        action="store_true",
        help="Skip scraping and use existing T1_Wildfire_Evacs.csv"
    )
    
    args = parser.parse_args()
    
    # Validate cutoff
    if not 0 <= args.cutoff <= 100:
        print("❌ ERROR: --cutoff must be between 0 and 100")
        sys.exit(1)
    
    # Run pipeline
    pipeline = PipelineOrchestrator(
        match_cutoff=args.cutoff,
        skip_scraping=args.skip_scraping
    )
    pipeline.run()


if __name__ == "__main__":
    main()