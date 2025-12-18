# main_pipeline.py

from t1_pipeline import scrape_wildfire_data, T1_URLS
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

    # use the zipped census file
    census_demo = build_census_lookup("98-401-X2021022_eng_CSV.zip")

    mapping_auto = auto_match_local_authorities(wildfire_df, census_demo, score_cutoff=80)

    wildfire_df = wildfire_df.merge(
        mapping_auto[["Local Authority", "DGUID"]],
        on="Local Authority",
        how="left",
    )

    wildfire_df = enrich_with_census(wildfire_df)

    print(wildfire_df.head())
    wildfire_df.to_csv("T1_Wildfire_Evacs_Enriched.csv", index=False)


if __name__ == "__main__":
    main()



