# manitoba_census.py

# READ CSV CENSUS DATA
import pandas as pd

# Read CSV file directly into a pandas DataFrame
df = pd.read_csv('csv files/Manitoba_2021_Census.csv')

# Display the first few rows of the data
print(df.head())
