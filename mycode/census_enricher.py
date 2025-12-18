# t1_census_pipeline.py

import pandas as pd
from thefuzz import process

def normalize_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    for pref in ["town of ", "city of ", "rm of ", "r.m. of ", "rural municipality of "]:
        if s.startswith(pref):
            s = s[len(pref):]
    return s.replace("  ", " ")

def build_census_lookup(census_path: str) -> pd.DataFrame:
    census = pd.read_csv(census_path)

    # total pop
    csd = census[census["GEO_LEVEL"] == "Census subdivision"].copy()
    census_pop = csd[csd["CHARACTERISTIC_NAME"] == "Population, 2021"].copy()
    census_pop = census_pop[["DGUID", "ALT_GEO_CODE", "GEO_NAME", "C1_COUNT_TOTAL"]]
    census_pop = census_pop.rename(columns={"C1_COUNT_TOTAL": "POP_2021"})
    census_pop["GEO_NAME_NORM"] = census_pop["GEO_NAME"].apply(normalize_name)

    # add Indigenous
    ind_tot = csd[csd["CHARACTERISTIC_NAME"] == "Indigenous identity"][
        ["DGUID", "C1_COUNT_TOTAL"]
    ].rename(columns={"C1_COUNT_TOTAL": "INDIG_POP_2021"})

    denom = csd[
        csd["CHARACTERISTIC_NAME"]
        == "Total population in private households by Indigenous identity"
    ][["DGUID", "C1_COUNT_TOTAL"]].rename(
        columns={"C1_COUNT_TOTAL": "INDIG_DENOM_2021"}
    )

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
    wildfire_df: pd.DataFrame, census_demo: pd.DataFrame, score_cutoff: int = 80
) -> pd.DataFrame:
    # unique LAs
    authorities = pd.Series(
        wildfire_df["Local Authority"].unique(), name="Local Authority"
    ).to_frame()
    authorities["LA_NORM"] = authorities["Local Authority"].apply(normalize_name)

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

        c_row = census_demo[census_demo["GEO_NAME_NORM"] == match].iloc[0]
        matches.append({
            "Local Authority": la,
            "match_score": score,
            "DGUID": c_row["DGUID"],
        })

    mapping_auto = pd.DataFrame(matches)
    return mapping_auto.merge(census_demo, on="DGUID", how="left")
