import polars as pl
import glob

SCHEMA = ["id", "title", "selftext", "author", "created_utc", "subreddit"]

dfs = []
files = glob.glob("ubc_chunks/ubc_*.parquet")

for f in files:
    df_c = pl.read_parquet(f)
    # ensure created_utc is Int64
    if df_c["created_utc"].dtype != pl.Int64:
        df_c = df_c.with_columns(pl.col("created_utc").cast(pl.Int64))
    dfs.append(df_c)

# Combine all chunks safely
df = pl.concat(dfs, rechunk=True)

import sqlite3

# Convert Polars DataFrame to Pandas (needed for sqlite3)
df_pd = df.to_pandas()

# Connect (or create) SQLite database
conn = sqlite3.connect("ubc_posts.db")

# Write DataFrame to table "posts"
df_pd.to_sql("posts", conn, if_exists="replace", index=False)

# Close connection
conn.close()

print("Saved to ubc_posts.db successfully!")
