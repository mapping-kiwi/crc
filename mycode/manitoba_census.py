import pandas as pd

# Read the zipped CSV file directly into a pandas DataFrame
df = pd.read_csv('98-401-X2021022_eng_CSV.zip', compression='zip')

# Display the first few rows of the data
print(df.head())
