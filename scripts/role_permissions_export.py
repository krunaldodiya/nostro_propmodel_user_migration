import polars as pl

# Input parameters
input_csv = "csv/input/role_permissions.csv"  # path to your input file
output_csv = "csv/output/new_role_permissions.csv"  # path for the output file
role_ids_to_keep = [
    "4498cf39-7fe2-4059-9571-6e65632eb283",
    "6385517c-60e2-4866-873d-7859a28c2de3",
    "f00e01b9-ffaa-49c6-b680-7348ef7797a4",
]  # list of role_ids you want to retain

# Read the CSV into a Polars DataFrame
df = pl.read_csv(input_csv)

# Filter rows where role_uuid is in the provided list
filtered_df = df.filter(pl.col("role_uuid").is_in(role_ids_to_keep))

# Write the filtered data to a new CSV
filtered_df.write_csv(output_csv)

print(f"Filtered CSV saved to: {output_csv}")
