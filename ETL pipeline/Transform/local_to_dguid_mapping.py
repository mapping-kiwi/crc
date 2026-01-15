# local_to_dguid_mapping.py

from typing import Dict


def load_local_to_dguid_mapping() -> Dict[str, str]:
    """
    Return a dict mapping Local Authority names to Census DGUIDs.
    Start with a few, expand as needed.
    """
    return {
        # example entries – adjust names to match your "Local Authority" column exactly
        "Flin Flon": "2021A00054621064",
        # "Cross Lake": "2021A0005462XXXX",
        # "Norway House": "2021A0005462YYYY",
    }
