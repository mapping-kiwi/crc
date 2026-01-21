# main_pipeline.py

from t1_manitoba.py import scrape_wildfire_data, T1_URLS
from t1_census_pipeline import (
    build_census_lookup,
    auto_match_local_authorities,
    enrich_with_census,
)
import pandas as pd


def main() -> None:
    wildfire_df = scrape_wildfire_data(T1_URLS)

    if "Local Authority" in wildfire_df.columns:
        wildfire_df["Local Authority"] = (
            wildfire_df["Local Authority"].replace("", pd.NA).ffill()
        )

    # Auto-match Local Authorities to census geographies
    census_path = "csv files/Manitoba_2021_Census.csv"
    wildfire_df = enrich_with_census(wildfire_df, census_path)

    print(wildfire_df.head())
    wildfire_df.to_csv("csv files/T1_Wildfire_Evacs_Enriched.csv", index=False)


if __name__ == "__main__":
    main()



