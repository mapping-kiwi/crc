# add at top with other imports
import json
from typing import Tuple, Dict, Any
import pandas as pd
import requests

CENSUS_PROFILE_API_BASE = (
    "https://api.statcan.gc.ca/census-recensement/profile/sdmx/rest/data/"
    "CPR_2021_CSD"  # census subdivision-level profile dataflow
)
# dimension order for this dataflow: GEO, SEX, CHAR, STAT [web:7]
# GEO: DGUID (e.g., 2021A00054621064)
# SEX: 1 = Total - Gender [web:7]
# STAT: 1 = Counts [web:7]

# characteristic codes (CHAR) you care about
CHAR_POP_2021 = "1"      # Population, 2021 [web:7]
CHAR_INDIG_TOTAL = "1910"  # Total - Indigenous identity for population in private households [web:4][web:27]


def fetch_census_values_for_dguid(dguid: str) -> tuple[int | None, int | None]:
    """
    Return (total_population_2021, total_indigenous_identity) for a given Census DGUID,
    using the 2021 Census Profile SDMX JSON API. Returns (None, None) if lookup fails. [web:7]
    """
    # SDMX key: GEO.SEX.CHAR.STAT
    # Use two observations: one for POP_2021, one for INDIG_TOTAL [web:7]
    char_list = f"{CHAR_POP_2021}+{CHAR_INDIG_TOTAL}"
    key = f"{dguid}.1.{char_list}.1"

    params = {
        "contentType": "json",   # get SDMX-JSON [web:7]
        "detail": "dataonly"
    }

    url = f"{CENSUS_PROFILE_API_BASE}/{key}"
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException:
        print(f"Failed Census API call for {dguid}")
        return None, None
    except json.JSONDecodeError:
        print(f"Bad JSON from Census API for {dguid}")
        return None, None

    # SDMX-JSON: observations are in dataSets[0]["observations"], with keys like "0:0:0:0" [web:7]
    obs = data.get("dataSets", [{}])[0].get("observations", {})
    if not obs:
        return None, None

    # Decode dimensions so we can tell which obs is which CHAR [web:7]
    dims = data.get("structure", {}).get("dimensions", {}).get("observation", [])
    char_dim_idx = next(
        (i for i, d in enumerate(dims) if d.get("id") == "CHAR"), None
    )
    if char_dim_idx is None:
        return None, None

    pop_val = None
    indig_val = None

    for key_str, val_arr in obs.items():
        # key_str is colon-separated index string (e.g., "0:0:0:0") [web:7]
        idx = key_str.split(":")
        if char_dim_idx >= len(idx):
            continue
        char_pos = int(idx[char_dim_idx])

        char_codes = dims[char_dim_idx]["values"]
        if char_pos >= len(char_codes):
            continue

        char_code = char_codes[char_pos]["id"]
        value = val_arr[0]  # first element is the numeric value [web:7]

        if char_code == CHAR_POP_2021:
            pop_val = value
        elif char_code == CHAR_INDIG_TOTAL:
            indig_val = value

    return pop_val, indig_val

def enrich_with_census(
    wildfire_df: pd.DataFrame,
    local_to_dguid: Dict[str, str],
) -> pd.DataFrame:
    """
    Add DGUID, Census_Pop_2021, Census_Indig_Total columns to wildfire_df.
    """
    # Map Local Authority → DGUID
    if "Local Authority" in wildfire_df.columns:
        wildfire_df = wildfire_df.copy()
        wildfire_df["Local Authority"] = (
            wildfire_df["Local Authority"].replace("", pd.NA).ffill()
        )
        wildfire_df["DGUID"] = wildfire_df["Local Authority"].map(local_to_dguid)
    else:
        wildfire_df["DGUID"] = pd.NA

    dguid_cache: Dict[str, Tuple[int | None, int | None]] = {}

    def _lookup(row: pd.Series) -> pd.Series:
        dguid = row.get("DGUID")
        if pd.isna(dguid):
            return pd.Series(
                {"Census_Pop_2021": None, "Census_Indig_Total": None}
            )

        dguid = str(dguid)
        if dguid not in dguid_cache:
            dguid_cache[dguid] = fetch_census_values_for_dguid(dguid)

        pop_2021, indig_total = dguid_cache[dguid]
        return pd.Series(
            {"Census_Pop_2021": pop_2021, "Census_Indig_Total": indig_total}
        )

    census_cols = wildfire_df.apply(_lookup, axis=1)
    wildfire_df = pd.concat([wildfire_df, census_cols], axis=1)
    return wildfire_df
