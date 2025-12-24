# CRC Project ⛑️
ETL Data Pipeline Development for the Canadian Red Cross. Automating wildfire evacuation orders to an ArcGIS Online webmap.Semester-long credit project (GEOG 291) at McGill University.

Usage:
'CSV files' Folder contains the output of running the script as well as the Manitoba 2021 Census from which census data was extracted. 

'my code' Folder contains modules for the pipeline. 

EXTRACT
Run 'open_manitoba_census' to open the 2021 MB Census;
Run 't1_pipeline' to scrape wildfire evacuation notices from MB wildfire authority. (Add urls to the dictionary as demonstrated to increase searches);


TRANSFORM & LOAD:
Run 't1_census_pipeline' to enrich T1 data with census information. The census enricher normalizes authority names by removing common prefixes to allow them to be matched with authorities as formatted in the MB Census. Populations are then extracted and added to the 'T1_Wildfire_Evacs_Enriched' CSV.

Run 'match_authority_to_census';

Run the main module; 

Open 'csv files' folder to view loaded and timestamped csv files.