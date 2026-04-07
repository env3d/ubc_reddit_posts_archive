import polars as pl
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import fsspec

INPUT_PATTERN = "datasets/fddemarco/pushshift-reddit/**/*.parquet"
OUTPUT_DIR = "ubc_chunks"
SCHEMA = ["id", "title", "selftext", "author", "created_utc", "subreddit"]
os.makedirs(OUTPUT_DIR, exist_ok=True)

fs = fsspec.filesystem("hf")

def process_chunk(file_path):
    chunk_name = os.path.basename(file_path)
    output_path = os.path.join(OUTPUT_DIR, f"ubc_{chunk_name}")
    if os.path.exists(output_path):
        return output_path  # skip already processed

    df = (
        pl.read_parquet(f"hf://{file_path}")
        .filter(pl.col("subreddit").str.to_lowercase() == "ubc")
        .select(SCHEMA)
    )
    if df.shape[0] == 0:
        df = pl.DataFrame({c: [] for c in SCHEMA})

    df.write_parquet(output_path, compression="snappy")
    return output_path

# ----------------------------
# Main guard (required on macOS / spawn)
# ----------------------------
if __name__ == "__main__":
    files = fs.glob(INPUT_PATTERN)
    files.sort()

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_chunk, f) for f in files]

        with tqdm(total=len(files), desc="Processing chunks") as pbar:
            for future in as_completed(futures):                
                _ = future.result()
                pbar.update(1)
                pbar.refresh()
