# export.py

"""
EXPORT MODULE FOR WILDFIRE EVACUATION ETL PIPELINE

Handles all file exports, report generation, and output organization.
Part of the LOAD stage in the ETL process.

Functions:
    - export_raw_data: Save raw HTML and text from scraping
    - export_scraped_data: Save scraped wildfire evacuation data
    - export_cleaned_data: Save cleaned datasets
    - export_enriched_data: Save final enriched wildfire data
    - export_qa_reports: Save all QA reports
    - export_all: Complete export pipeline
"""

import pandas as pd
from typing import Optional, Dict, Any
from io_paths import IOPaths


class ExportManager:
    """Manage all data exports for the ETL pipeline."""
    
    def __init__(self, paths: IOPaths):
        """
        Initialize export manager with IO paths configuration.
        
        Args:
            paths (IOPaths): Configured IO paths object
        """
        self.paths = paths
        self.export_log = []
    
    def _log_export(self, description: str, path: str, record_count: Optional[int] = None):
        """
        Log an export operation.
        
        Args:
            description (str): Description of what was exported
            path (str): File path where data was saved
            record_count (int, optional): Number of records exported
        """
        log_entry = {
            "description": description,
            "path": path,
            "record_count": record_count
        }
        self.export_log.append(log_entry)
        
        if record_count is not None:
            print(f"  ✓ {description}: {path} ({record_count} records)")
        else:
            print(f"  ✓ {description}: {path}")
    
    def export_raw_html(self, source_name: str, html_content: str):
        """
        Export raw HTML from a data source.
        
        Args:
            source_name (str): Name of the data source
            html_content (str): Raw HTML content to save
        """
        path = self.paths.raw_html_path(source_name)
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        self._log_export(f"Raw HTML ({source_name})", path)
    
    def export_raw_text(self, text_content: str):
        """
        Export processed text from all sources.
        
        Args:
            text_content (str): Combined text content from all sources
        """
        path = self.paths.raw_text_path()
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(text_content)
        
        self._log_export("Processed text", path)
    
    def export_scraped_metadata(self, metadata_df: pd.DataFrame):
        """
        Export metadata from scraping process.
        
        Args:
            metadata_df (pd.DataFrame): Metadata from web scraping
        """
        path = self.paths.scraped_metadata
        metadata_df.to_csv(path, index=False)
        
        self._log_export("Scraping metadata", path, len(metadata_df))
    
    def export_scraped_wildfire(self, wildfire_df: pd.DataFrame):
        """
        Export scraped wildfire evacuation data (both latest and versioned).
        
        Args:
            wildfire_df (pd.DataFrame): Scraped wildfire evacuation data
        """
        # Save latest (overwrites)
        latest_path = self.paths.scraped_wildfire_latest
        wildfire_df.to_csv(latest_path, index=False)
        self._log_export("Scraped wildfire (latest)", latest_path, len(wildfire_df))
        
        # Save versioned
        versioned_path = self.paths.scraped_wildfire_versioned
        wildfire_df.to_csv(versioned_path, index=False)
        self._log_export("Scraped wildfire (versioned)", versioned_path, len(wildfire_df))
    
    def export_cleaned_wildfire(self, cleaned_df: pd.DataFrame):
        """
        Export cleaned wildfire evacuation data.
        
        Args:
            cleaned_df (pd.DataFrame): Cleaned wildfire data
        """
        path = self.paths.cleaned_wildfire
        cleaned_df.to_csv(path, index=False)
        
        self._log_export("Cleaned wildfire data", path, len(cleaned_df))
    
    def export_cleaned_census(self, census_df: pd.DataFrame):
        """
        Export cleaned census lookup table.
        
        Args:
            census_df (pd.DataFrame): Cleaned census demographic data
        """
        path = self.paths.cleaned_census
        census_df.to_csv(path, index=False)
        
        self._log_export("Cleaned census lookup", path, len(census_df))
    
    def export_matching_outputs(
        self, 
        mapping_df: pd.DataFrame,
        unmatched_df: Optional[pd.DataFrame] = None,
        low_confidence_df: Optional[pd.DataFrame] = None
    ):
        """
        Export all matching-related outputs.
        
        Args:
            mapping_df (pd.DataFrame): Authority-to-DGUID mapping
            unmatched_df (pd.DataFrame, optional): Unmatched authorities
            low_confidence_df (pd.DataFrame, optional): Low-confidence matches
        """
        # Export main mapping
        path = self.paths.authority_mapping
        mapping_df.to_csv(path, index=False)
        self._log_export("Authority-to-DGUID mapping", path, len(mapping_df))
        
        # Export unmatched authorities
        if unmatched_df is not None and not unmatched_df.empty:
            path = self.paths.unmatched_authorities
            unmatched_df.to_csv(path, index=False)
            self._log_export("Unmatched authorities", path, len(unmatched_df))
        
        # Export low-confidence matches
        if low_confidence_df is not None and not low_confidence_df.empty:
            path = self.paths.low_confidence_matches
            low_confidence_df.to_csv(path, index=False)
            self._log_export("Low-confidence matches", path, len(low_confidence_df))
    
    def export_enriched_wildfire(self, enriched_df: pd.DataFrame):
        """
        Export final enriched wildfire data (both latest and versioned).
        
        Args:
            enriched_df (pd.DataFrame): Wildfire data enriched with census demographics
        """
        # Save latest (overwrites)
        latest_path = self.paths.enriched_wildfire_latest
        enriched_df.to_csv(latest_path, index=False)
        self._log_export("Enriched wildfire (latest)", latest_path, len(enriched_df))
        
        # Save versioned
        versioned_path = self.paths.enriched_wildfire_versioned
        enriched_df.to_csv(versioned_path, index=False)
        self._log_export("Enriched wildfire (versioned)", versioned_path, len(enriched_df))
    
    def export_authority_audit(self, audit_df: pd.DataFrame):
        """
        Export authority frequency audit.
        
        Args:
            audit_df (pd.DataFrame): Authority audit with frequency counts
        """
        path = self.paths.authority_audit
        audit_df.to_csv(path, index=False)
        
        self._log_export("Authority audit", path, len(audit_df))
    
    def export_qa_report(self, report_content: str, report_type: str = "pipeline"):
        """
        Export QA report to file.
        
        Args:
            report_content (str): Report content to save
            report_type (str): Type of report ('scraping', 'matching', or 'pipeline')
        """
        if report_type == "scraping":
            path = self.paths.qa_report_scraping
        elif report_type == "matching":
            path = self.paths.qa_report_matching
        else:
            path = self.paths.qa_report_pipeline
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(report_content)
        
        self._log_export(f"QA Report ({report_type})", path)
    
    def generate_export_summary(self) -> str:
        """
        Generate summary of all exports performed.
        
        Returns:
            str: Formatted export summary
        """
        summary = ["\n" + "="*60]
        summary.append("EXPORT SUMMARY")
        summary.append("="*60)
        summary.append(f"Run Timestamp: {self.paths.run_timestamp}")
        summary.append(f"Total Exports: {len(self.export_log)}")
        summary.append("\n[EXPORTED FILES]")
        
        for i, entry in enumerate(self.export_log, 1):
            if entry["record_count"] is not None:
                summary.append(
                    f"  {i}. {entry['description']} ({entry['record_count']} records)"
                )
            else:
                summary.append(f"  {i}. {entry['description']}")
            summary.append(f"     → {entry['path']}")
        
        summary.append("="*60 + "\n")
        return "\n".join(summary)
    
    def save_export_summary(self):
        """Save export summary to file."""
        summary = self.generate_export_summary()
        path = self.paths.qa_report_pipeline
        
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n\n" + summary)
        
        print(summary)


# CONVENIENCE FUNCTIONS

def export_raw_data(
    paths: IOPaths,
    html_data: Dict[str, str],
    text_data: str,
    metadata_df: pd.DataFrame
) -> ExportManager:
    """
    Export all raw data from scraping phase.
    
    Args:
        paths (IOPaths): IO paths configuration
        html_data (Dict[str, str]): Dictionary mapping source names to HTML content
        text_data (str): Combined processed text from all sources
        metadata_df (pd.DataFrame): Scraping metadata
        
    Returns:
        ExportManager: Manager with export log
    """
    print("\n[EXPORTING RAW DATA]")
    manager = ExportManager(paths)
    
    # Export raw HTML for each source
    for source_name, html_content in html_data.items():
        manager.export_raw_html(source_name, html_content)
    
    # Export processed text
    manager.export_raw_text(text_data)
    
    # Export metadata
    manager.export_scraped_metadata(metadata_df)
    
    return manager


def export_scraped_data(
    paths: IOPaths,
    wildfire_df: pd.DataFrame
) -> ExportManager:
    """
    Export scraped wildfire evacuation data.
    
    Args:
        paths (IOPaths): IO paths configuration
        wildfire_df (pd.DataFrame): Scraped wildfire data
        
    Returns:
        ExportManager: Manager with export log
    """
    print("\n[EXPORTING SCRAPED DATA]")
    manager = ExportManager(paths)
    manager.export_scraped_wildfire(wildfire_df)
    
    return manager


def export_cleaned_data(
    paths: IOPaths,
    wildfire_df: pd.DataFrame,
    census_df: pd.DataFrame
) -> ExportManager:
    """
    Export cleaned datasets.
    
    Args:
        paths (IOPaths): IO paths configuration
        wildfire_df (pd.DataFrame): Cleaned wildfire data
        census_df (pd.DataFrame): Cleaned census data
        
    Returns:
        ExportManager: Manager with export log
    """
    print("\n[EXPORTING CLEANED DATA]")
    manager = ExportManager(paths)
    
    manager.export_cleaned_wildfire(wildfire_df)
    manager.export_cleaned_census(census_df)
    
    return manager


def export_enriched_data(
    paths: IOPaths,
    enriched_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    unmatched_df: Optional[pd.DataFrame] = None,
    low_confidence_df: Optional[pd.DataFrame] = None,
    audit_df: Optional[pd.DataFrame] = None
) -> ExportManager:
    """
    Export final enriched data and all matching outputs.
    
    Args:
        paths (IOPaths): IO paths configuration
        enriched_df (pd.DataFrame): Final enriched wildfire data
        mapping_df (pd.DataFrame): Authority-to-DGUID mapping
        unmatched_df (pd.DataFrame, optional): Unmatched authorities
        low_confidence_df (pd.DataFrame, optional): Low-confidence matches
        audit_df (pd.DataFrame, optional): Authority frequency audit
        
    Returns:
        ExportManager: Manager with export log
    """
    print("\n[EXPORTING ENRICHED DATA]")
    manager = ExportManager(paths)
    
    # Export enriched wildfire data
    manager.export_enriched_wildfire(enriched_df)
    
    # Export matching outputs
    manager.export_matching_outputs(mapping_df, unmatched_df, low_confidence_df)
    
    # Export authority audit if provided
    if audit_df is not None:
        manager.export_authority_audit(audit_df)
    
    return manager


def export_qa_reports(
    paths: IOPaths,
    scraping_report: Optional[str] = None,
    matching_report: Optional[str] = None,
    pipeline_report: Optional[str] = None
) -> ExportManager:
    """
    Export all QA reports.
    
    Args:
        paths (IOPaths): IO paths configuration
        scraping_report (str, optional): Scraping QA report content
        matching_report (str, optional): Matching QA report content
        pipeline_report (str, optional): Overall pipeline QA report
        
    Returns:
        ExportManager: Manager with export log
    """
    print("\n[EXPORTING QA REPORTS]")
    manager = ExportManager(paths)
    
    if scraping_report:
        manager.export_qa_report(scraping_report, "scraping")
    
    if matching_report:
        manager.export_qa_report(matching_report, "matching")
    
    if pipeline_report:
        manager.export_qa_report(pipeline_report, "pipeline")
    
    return manager


def export_all(
    paths: IOPaths,
    enriched_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    qa_reports: Dict[str, str],
    unmatched_df: Optional[pd.DataFrame] = None,
    low_confidence_df: Optional[pd.DataFrame] = None,
    audit_df: Optional[pd.DataFrame] = None
):
    """
    Complete export pipeline - save all outputs and generate summary.
    
    Args:
        paths (IOPaths): IO paths configuration
        enriched_df (pd.DataFrame): Final enriched wildfire data
        mapping_df (pd.DataFrame): Authority-to-DGUID mapping
        qa_reports (Dict[str, str]): Dictionary of QA reports by type
        unmatched_df (pd.DataFrame, optional): Unmatched authorities
        low_confidence_df (pd.DataFrame, optional): Low-confidence matches
        audit_df (pd.DataFrame, optional): Authority frequency audit
    """
    print("\n" + "="*60)
    print("EXPORTING ALL PIPELINE OUTPUTS")
    print("="*60)
    
    manager = ExportManager(paths)
    
    # Export enriched data
    manager.export_enriched_wildfire(enriched_df)
    manager.export_matching_outputs(mapping_df, unmatched_df, low_confidence_df)
    
    if audit_df is not None:
        manager.export_authority_audit(audit_df)
    
    # Export QA reports
    for report_type, content in qa_reports.items():
        if content:
            manager.export_qa_report(content, report_type)
    
    # Generate and save export summary
    manager.save_export_summary()
    
    print("\n✓ All exports complete!")


if __name__ == "__main__":
    # Example usage
    from io_paths import IOPaths
    
    print("="*60)
    print("EXPORT MODULE - STANDALONE TEST")
    print("="*60)
    
    # Initialize paths
    paths = IOPaths()
    
    # Create sample data
    sample_df = pd.DataFrame({
        'Local Authority': ['Test Community'],
        'DGUID': ['2021A00054621064'],
        'Census_Pop_2021': [5000]
    })
    
    # Test export
    manager = ExportManager(paths)
    manager.export_enriched_wildfire(sample_df)
    
    print(manager.generate_export_summary())