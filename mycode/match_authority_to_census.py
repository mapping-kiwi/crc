#match_authority_to_census.py

# Automated fuzzy matching of local authority names to census subdivision names
# joining census data to wildfire evac zones
import pandas as pd
from thefuzz import process

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
