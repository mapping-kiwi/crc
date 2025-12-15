from t1_census_pipeline import enrich_with_census
from t1_pipeline import scrape_wildfire_data, T1_URLS
from local_to_dguid_mapping import load_local_to_dguid_mapping

def main() -> None :
    
    load_local_to_dguid = load_local_to_dguid_mapping()

    # Load Local Authority to DGUID mapping
    wildfire_df = scrape_wildfire_data(T1_URLS)
    # clean Local Authority, etc.
    wildfire_df = enrich_with_census(wildfire_df, load_local_to_dguid)
    
    # For now: print and save a CSV for ArcGIS
    print(wildfire_df.head())
    wildfire_df.to_csv("T1_Wildfire_Evacs_Enriched.csv", index=False)


if __name__ == "__main__":
    main()
