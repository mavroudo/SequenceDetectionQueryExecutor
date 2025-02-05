import pandas as pd

# Replace 'input.parquet' with your Parquet file path
parquet_file = '4.parquet'
csv_file = '4.csv'

# Read the Parquet file
df = pd.read_parquet(parquet_file)

# Write the DataFrame to a CSV file
df.to_csv(csv_file, index=False)

print(f"Parquet file has been converted to {csv_file}")
