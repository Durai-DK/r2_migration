import math
import time
import pandas as pd

FILES = [
    "Mobiles_28-29m.xlsx",
    "Mobiles_29-30m.xlsx",
    "Mobiles_30-31m.xlsx",
    "Mobiles_31-32m.xlsx",
    "Mobiles_32-33m.xlsx",
    "Mobiles_33-34m.xlsx",
    "Mobiles_34-35m.xlsx",
    "Mobiles_35-36m.xlsx",
    "Mobiles_36-37m.xlsx",
    "Mobiles_37-38m.xlsx",
    "Mobiles_38-39m.xlsx",
    "Mobiles_39-40m.xlsx",
]


COL = "mobiles"
CHUNK_SIZE = 1_000_000

start_time = time.time()

# ---------------- READ & CLEAN ----------------
dfs = []
total_read = 0

for idx, file in enumerate(FILES, start=1):
    df = pd.read_excel(file, dtype=str, engine="openpyxl")
    rows = len(df)
    total_read += rows

    print(f"📥 [{idx}/{len(FILES)}] {file} → {rows:,} rows")

    # Keep only required column
    df = df[[COL]]

    # Clean mobile numbers
    df[COL] = df[COL].astype(str).str.strip()

    # Remove blanks / NaN / junk
    df = df[df[COL].notna()]
    df = df[df[COL] != ""]         # keep only digits

    dfs.append(df)

print(f"\n📊 Total rows read (before dedupe): {total_read:,}")

# ---------------- COMBINE ----------------
combined = pd.concat(dfs, ignore_index=True)
print(f"📦 Combined rows: {len(combined):,}")

# ---------------- DEDUPLICATE ----------------
combined_unique = combined.drop_duplicates(subset=[COL], keep="first")
total_unique = len(combined_unique)

print(f"✨ Unique mobiles: {total_unique:,}")

# ---------------- SPLIT & SAVE ----------------
num_files = math.ceil(total_unique / CHUNK_SIZE)
print(f"\n📂 Splitting into {num_files} Excel files (1M each)")

for i in range(num_files):
    start = i * CHUNK_SIZE
    end = start + CHUNK_SIZE

    chunk = combined_unique.iloc[start:end]

    output_file = f"output_mobiles_part_{i+1}.xlsx"
    chunk.to_excel(output_file, index=False, engine="openpyxl")

    print(
        f"✅ Saved {output_file} | "
        f"Rows: {len(chunk):,} | "
        f"Range: {start:,} → {min(end, total_unique):,}"
    )

# ---------------- SUMMARY ----------------
elapsed = round(time.time() - start_time, 2)

print("\n🎉 PROCESS COMPLETED SUCCESSFULLY")
print(f"⏱ Time taken: {elapsed} seconds")
print(f"📁 Output files: {num_files}")
print(f"📞 Total unique mobiles: {total_unique:,}")
