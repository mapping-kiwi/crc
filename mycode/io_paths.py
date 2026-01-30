# io_paths.py

"""
IO PATHS MODULE FOR WILDFIRE EVACUATION ETL PIPELINE

Centralized configuration for all input/output paths, directory creation,
and file naming with timestamps.

Ensures consistent file organization across the entire ETL pipeline.
"""

import os
from datetime import datetime, UTC
from typing import Optional
from pathlib import Path

class IOPaths:
    """Manage all file paths and directories for the ETL pipeline."""
    
    def __init__(self):
        """Initialize all file paths with timestamp."""
        self.run_timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        
        # Base directories - move data outside mycode
        self.base_dir = Path(__file__).parent.parent  # Go up to 'crc' directory
        self.csv_dir = self.base_dir / "csv files"
        self.qa_dir = self.base_dir / "qa_reports"
        self.raw_html_dir = self.base_dir / "raw_html"
        self.raw_text_dir = self.base_dir / "raw_text"
        self.archive_dir = self.base_dir / "archive"
        self.qa_reports_dir = os.path.join(self.base_dir, "qa_reports")
        os.makedirs(self.qa_reports_dir, exist_ok=True)

        # Create directories if they don't exist
        for directory in [self.csv_dir, self.qa_dir, self.raw_html_dir, 
                        self.raw_text_dir, self.archive_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _create_directories(self):
        """Create all required output directories."""
        directories = [
            self.csv_dir,
            self.raw_html_dir,
            self.raw_text_dir,
            self.qa_reports_dir,
            self.archive_dir,
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    # ==================== INPUT PATHS ====================
    
    @property
    def census_input(self) -> str:
        """Path to input census CSV file."""
        return os.path.join(self.csv_dir, "Manitoba_2021_Census.csv")
    
    @property
    def wildfire_input(self) -> str:
        """Path to latest scraped wildfire data (if exists)."""
        return os.path.join(self.csv_dir, "T1_Wildfire_Evacs.csv")
    
    # ==================== RAW DATA OUTPUTS ====================
    
    def raw_html_path(self, source_name: str) -> str:
        """
        Path for saving raw HTML from a source.
        
        Args:
            source_name (str): Name of the data source
            
        Returns:
            str: Full path for HTML file
        """
        safe_name = source_name.replace(" ", "_")
        filename = f"{safe_name}_{self.run_timestamp}.html"
        return os.path.join(self.raw_html_dir, filename)
    
    def raw_text_path(self) -> str:
        """Path for saving processed text from all sources."""
        filename = f"T1_raw_{self.run_timestamp}.txt"
        return os.path.join(self.raw_text_dir, filename)
    
    # ==================== SCRAPED DATA OUTPUTS ====================
    
    @property
    def scraped_metadata(self) -> str:
        """Path for metadata from scraping process."""
        return os.path.join(self.csv_dir, "T1_Data.csv")
    
    @property
    def scraped_wildfire_latest(self) -> str:
        """Path for latest wildfire evacuation data (overwrites)."""
        return os.path.join(self.csv_dir, "T1_Wildfire_Evacs.csv")
    
    @property
    def scraped_wildfire_versioned(self) -> str:
        """Path for versioned wildfire evacuation data."""
        filename = f"T1_Wildfire_Evacs_{self.run_timestamp}.csv"
        return os.path.join(self.csv_dir, filename)
    
    # ==================== CLEANED DATA OUTPUTS ====================
    
    @property
    def cleaned_wildfire(self) -> str:
        """Path for cleaned wildfire data."""
        return os.path.join(self.csv_dir, "T1_Wildfire_Evacs_cleaned.csv")
    
    @property
    def cleaned_census(self) -> str:
        """Path for cleaned census lookup table."""
        return os.path.join(self.csv_dir, "census_lookup_cleaned.csv")
    
    # ==================== MATCHING OUTPUTS ====================
    
    @property
    def authority_mapping(self) -> str:
        """Path for authority-to-DGUID mapping table."""
        return os.path.join(self.csv_dir, "authority_to_dguid_mapping.csv")
    
    @property
    def unmatched_authorities(self) -> str:
        """Path for list of unmatched authorities."""
        return os.path.join(self.csv_dir, "unmatched_authorities.csv")
    
    @property
    def low_confidence_matches(self) -> str:
        """Path for low-confidence matches requiring review."""
        return os.path.join(self.csv_dir, "low_confidence_matches.csv")
    
    # ==================== ENRICHED DATA OUTPUTS ====================
    
    @property
    def enriched_wildfire_latest(self) -> str:
        """Path for latest enriched wildfire data (overwrites)."""
        return os.path.join(self.csv_dir, "T1_Wildfire_Evacs_Enriched.csv")
    
    @property
    def enriched_wildfire_versioned(self) -> str:
        """Path for versioned enriched wildfire data."""
        filename = f"T1_Wildfire_Evacs_Enriched_{self.run_timestamp}.csv"
        return os.path.join(self.csv_dir, filename)
    
    # ==================== QA REPORT OUTPUTS ====================
    
    @property
    def qa_report_scraping(self) -> str:
        """Path for scraping QA report."""
        filename = f"QA_Scraping_{self.run_timestamp}.txt"
        return os.path.join(self.qa_reports_dir, filename)
    
    @property
    def qa_report_matching(self) -> str:
        """Path for matching QA report."""
        filename = f"QA_Matching_{self.run_timestamp}.txt"
        return os.path.join(self.qa_reports_dir, filename)
    
    @property
    def qa_report_pipeline(self) -> str:
        """Path for overall pipeline QA report."""
        filename = f"QA_Pipeline_{self.run_timestamp}.txt"
        return os.path.join(self.qa_reports_dir, filename)
    
    @property
    def authority_audit(self) -> str:
        """Path for authority frequency audit."""
        return os.path.join(self.csv_dir, "authority_audit.csv")
    
    # ==================== ARCHIVE OUTPUTS ====================
    
    def archive_path(self, filename: str) -> str:
        """
        Create path for archiving a file.
        
        Args:
            filename (str): Name of file to archive
            
        Returns:
            str: Full path in archive directory
        """
        return os.path.join(self.archive_dir, filename)
    
    # ==================== UTILITY METHODS ====================
    
    def get_all_outputs(self) -> dict:
        """
        Get dictionary of all output paths for this run.
        
        Returns:
            dict: Dictionary mapping output names to file paths
        """
        return {
            "Raw HTML": "see raw_html_path(source_name)",
            "Raw Text": self.raw_text_path(),
            "Scraped Metadata": self.scraped_metadata,
            "Scraped Wildfire (Latest)": self.scraped_wildfire_latest,
            "Scraped Wildfire (Versioned)": self.scraped_wildfire_versioned,
            "Cleaned Wildfire": self.cleaned_wildfire,
            "Cleaned Census": self.cleaned_census,
            "Authority Mapping": self.authority_mapping,
            "Unmatched Authorities": self.unmatched_authorities,
            "Low Confidence Matches": self.low_confidence_matches,
            "Enriched Wildfire (Latest)": self.enriched_wildfire_latest,
            "Enriched Wildfire (Versioned)": self.enriched_wildfire_versioned,
            "QA Report - Scraping": self.qa_report_scraping,
            "QA Report - Matching": self.qa_report_matching,
            "QA Report - Pipeline": self.qa_report_pipeline,
            "Authority Audit": self.authority_audit,
        }
    
    def print_summary(self):
        """Print a summary of all configured paths."""
        print("\n" + "="*60)
        print("IO PATHS CONFIGURATION")
        print("="*60)
        print(f"Run Timestamp: {self.run_timestamp}")
        print(f"\nBase Directory: {self.base_dir}")
        print(f"\nOutput Directories:")
        print(f"  - CSV Files: {self.csv_dir}")
        print(f"  - Raw HTML: {self.raw_html_dir}")
        print(f"  - Raw Text: {self.raw_text_dir}")
        print(f"  - QA Reports: {self.qa_reports_dir}")
        print(f"  - Archive: {self.archive_dir}")
        print("="*60 + "\n")


if __name__ == "__main__":
    # Test the IO paths configuration
    paths = IOPaths()
    paths.print_summary()
    
    print("Sample Output Paths:")
    print(f"  Census Input: {paths.census_input}")
    print(f"  Enriched Output (Latest): {paths.enriched_wildfire_latest}")
    print(f"  Enriched Output (Versioned): {paths.enriched_wildfire_versioned}")
    print(f"  QA Report: {paths.qa_report_pipeline}")
    print(f"  Raw HTML (Manitoba): {paths.raw_html_path('Manitoba Evacs')}")