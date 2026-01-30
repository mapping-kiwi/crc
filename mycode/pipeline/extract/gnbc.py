"""
GNBC (GEOGRAPHICAL NAMES BOARD OF CANADA) DATA FETCHER
Fetches official place names from Natural Resources Canada's database.
"""

import requests
import pandas as pd
from typing import Optional, List
import time


def normalize_name(name: str) -> str:
    """Normalize place names for matching."""
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    prefixes = ["town of ", "city of ", "rm of ", "r.m. of ", "rural municipality of ", 
                "municipality of ", "village of ", "northern village of ", "provincial park"]
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    return " ".join(name.split())


class GNBCQASignals:
    """Track QA signals for GNBC data fetching."""
    
    def __init__(self):
        self.signals = {}
    
    def add(self, key: str, value):
        self.signals[key] = value
    
    def report(self) -> str:
        lines = ["[GNBC API QA SIGNALS]"]
        for key, value in self.signals.items():
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)


def fetch_gnbc_manitoba(
    entity_types: Optional[List[str]] = None,
    qa_signals: Optional[GNBCQASignals] = None
) -> pd.DataFrame:
    """Fetch Manitoba place names from GNBC database."""
    
    if qa_signals is None:
        qa_signals = GNBCQASignals()
    
    if entity_types is None:
        entity_types = ['LAKE', 'PARK', 'POPULATED PLACE', 'RESERVE', 'MISC']
    
    qa_signals.add('data_source', 'NRCan Geographical Names Database')
    qa_signals.add('province', 'Manitoba')
    qa_signals.add('entity_types_requested', ', '.join(entity_types))
    
    try:
        gnbc_df = fetch_via_api(entity_types, qa_signals)
        qa_signals.add('fetch_method', 'API - GNBC Web Service')
        return gnbc_df
    except Exception as e:
        qa_signals.add('api_error', str(e))
        qa_signals.add('fetch_method', 'Fallback - Embedded Data')
        return fetch_via_fallback(qa_signals)


def fetch_via_api(entity_types: List[str], qa_signals: GNBCQASignals) -> pd.DataFrame:
    """Fetch from GNBC web service API."""
    
    base_url = "https://geogratis.gc.ca/services/geoname/en/geonames"
    all_places = []
    
    for entity_type in entity_types:
        print(f"  → Fetching {entity_type} from GNBC...")
        
        params = {
            'q': '*',
            'province': 'MB',
            'theme': entity_type,
            'concise': 'province',
            'num': 1000,
            'output': 'summary'
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'items' in data:
                places = data['items']
                all_places.extend(places)
                print(f"    ✓ Found {len(places)} {entity_type} entries")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    ⚠️ Failed to fetch {entity_type}: {e}")
            continue
    
    if not all_places:
        raise ValueError("No data retrieved from GNBC API")
    
    df = pd.DataFrame(all_places)
    qa_signals.add('total_places_retrieved', len(df))
    df = process_gnbc_data(df, qa_signals)
    
    return df


def process_gnbc_data(df: pd.DataFrame, qa_signals: GNBCQASignals) -> pd.DataFrame:
    """Process and standardize GNBC data."""
    
    column_mapping = {
        'name': 'place_name',
        'generic': 'generic_category',
        'theme': 'entity_type',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'province': 'province',
        'location': 'location_description'
    }
    
    rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=rename_dict)
    
    if 'place_name' not in df.columns and 'name' in df.columns:
        df['place_name'] = df['name']
    
    df['place_name_normalized'] = df['place_name'].apply(normalize_name)
    df['data_source'] = 'GNBC'
    df['is_designated_place'] = True
    df['population'] = 0
    df['indigenous_population'] = 0
    
    def get_notes(row):
        entity = row.get('entity_type', 'UNKNOWN')
        generic = row.get('generic_category', '')
        
        if entity == 'LAKE':
            return 'Natural water body - uninhabited'
        elif entity == 'PARK':
            return f'{generic} - uninhabited wilderness'
        elif entity == 'POPULATED PLACE':
            return 'Unincorporated community'
        elif entity == 'RESERVE':
            return 'First Nations reserve - see census for population'
        else:
            return f'Designated place - {generic}'
    
    df['notes'] = df.apply(get_notes, axis=1)
    qa_signals.add('places_processed', len(df))
    
    return df


def fetch_via_fallback(qa_signals: GNBCQASignals) -> pd.DataFrame:
    """Fallback: Embedded GNBC data for Manitoba designated places."""
    
    gnbc_places = [
        # LAKES
        {'place_name': 'Nopiming Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake', 
         'latitude': 51.2, 'longitude': -95.1, 'notes': 'Within Nopiming Provincial Park'},
        {'place_name': 'Island Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 53.85, 'longitude': -94.65, 'notes': 'Large lake - Island Lake region'},
        {'place_name': 'Payuk Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.9, 'longitude': -97.8, 'notes': 'Remote northern lake'},
        {'place_name': 'Schist Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.8, 'longitude': -97.9, 'notes': 'Remote northern lake'},
        {'place_name': 'Twin Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.85, 'longitude': -97.85, 'notes': 'Remote northern lake'},
        {'place_name': 'Burge Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.87, 'longitude': -97.82, 'notes': 'Remote northern lake'},
        {'place_name': 'Zed Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.88, 'longitude': -97.88, 'notes': 'Remote northern lake'},
        {'place_name': 'White Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 55.7, 'longitude': -98.1, 'notes': 'Northwest region lake'},
        {'place_name': 'Whitefish Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 51.8, 'longitude': -100.5, 'notes': 'Lake near Camperville'},
        {'place_name': 'Lake Athapapuskow', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 54.8, 'longitude': -101.7, 'notes': 'Large lake with cottages'},
        {'place_name': 'Wallace Lake', 'entity_type': 'LAKE', 'generic_category': 'Lake',
         'latitude': 54.75, 'longitude': -101.8, 'notes': 'Cottage area near Flin Flon'},
        
        # PARKS
        {'place_name': 'Nopiming Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 51.1, 'longitude': -95.2, 'notes': 'Wilderness provincial park - 1,430 sq km'},
        {'place_name': 'Whiteshell Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 49.8, 'longitude': -95.2, 'notes': 'Provincial park - 2,729 sq km'},
        {'place_name': 'Atikaki Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 52.5, 'longitude': -95.0, 'notes': 'Wilderness provincial park - 3,981 sq km'},
        {'place_name': 'Atikaki Provincial Park (South Portion)', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 52.3, 'longitude': -95.0, 'notes': 'South portion of Atikaki Provincial Park'},
        {'place_name': 'Wekusko Falls Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 54.8, 'longitude': -99.9, 'notes': 'Provincial park near Snow Lake'},
        {'place_name': 'Grass River Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 54.7, 'longitude': -100.1, 'notes': 'Provincial park - 2,277 sq km'},
        {'place_name': 'Bakers Narrows Provincial Park', 'entity_type': 'PARK', 'generic_category': 'Provincial Park',
         'latitude': 54.72, 'longitude': -101.85, 'notes': 'Recreational park near Flin Flon'},
        
        # POPULATED PLACES
        {'place_name': 'Herb Lake Landing', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Hamlet',
         'latitude': 56.2, 'longitude': -98.4, 'notes': 'Unincorporated northern community'},
        {'place_name': 'Kelsey', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Locality',
         'latitude': 56.05, 'longitude': -96.5, 'notes': 'Small northern locality'},
        {'place_name': 'Granville Lake', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Locality',
         'latitude': 56.3, 'longitude': -100.7, 'notes': 'Remote northern community'},
        {'place_name': 'Sherridon', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Locality',
         'latitude': 55.13, 'longitude': -101.08, 'notes': 'Former mining community'},
        {'place_name': 'Community of Sherridon', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Locality',
         'latitude': 55.13, 'longitude': -101.08, 'notes': 'Former mining community'},
        {'place_name': 'Cormorant', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Hamlet',
         'latitude': 53.38, 'longitude': -101.08, 'notes': 'Small hamlet'},
        {'place_name': 'Bissett', 'entity_type': 'POPULATED PLACE', 'generic_category': 'Hamlet',
         'latitude': 51.03, 'longitude': -95.67, 'notes': 'Mining community'},
    ]
    
    df = pd.DataFrame(gnbc_places)
    df['place_name_normalized'] = df['place_name'].apply(normalize_name)
    df['data_source'] = 'GNBC_FALLBACK'
    df['is_designated_place'] = True
    df['population'] = 0
    df['indigenous_population'] = 0
    df['province'] = 'MB'
    
    qa_signals.add('places_processed', len(df))
    qa_signals.add('fallback_dataset_used', True)
    
    print(f"  ✓ Loaded {len(df)} designated places from fallback")
    
    return df


if __name__ == "__main__":
    print("Testing GNBC data fetcher...\n")
    
    try:
        qa_signals = GNBCQASignals()
        gnbc_data = fetch_gnbc_manitoba(qa_signals=qa_signals)
        
        print("\n" + "="*60)
        print("GNBC DESIGNATED PLACES")
        print("="*60)
        print(gnbc_data[['place_name', 'entity_type', 'generic_category']].head(20))
        
        print("\n" + "="*60)
        print("ENTITY TYPE BREAKDOWN")
        print("="*60)
        print(gnbc_data['entity_type'].value_counts())
        
        print("\n" + "="*60)
        print(qa_signals.report())
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
