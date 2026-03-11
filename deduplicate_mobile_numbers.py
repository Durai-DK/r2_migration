import json, pandas as pd, os, math

INPUT_FILE = "/Users/DK/Documents/Transaction_202601211103.json"
OUTPUT_DIR = "/Users/DK/Documents/unique_mobile_excels"
CHUNK_SIZE = 1_000_000

os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

df = pd.DataFrame(data["mobiles"])

df["customer_mobile__c"] = df["customer_mobile__c"].astype(str).str.strip()

unique_mobiles = df["customer_mobile__c"].dropna().nunique()

unique_df = df[["customer_mobile__c"]].dropna().drop_duplicates().reset_index(drop=True)

total_records = len(unique_df)

total_files = math.ceil(total_records / CHUNK_SIZE)

print(f"✅ Total unique mobiles: {total_records}")

for i in range(total_files):
    start = i * CHUNK_SIZE
    end = start + CHUNK_SIZE

    chunk_df = unique_df.iloc[start:end]

    output_file = os.path.join(
        OUTPUT_DIR,
        f"unique_mobiles_part_{i + 1}.xlsx"
    )

    chunk_df.to_excel(output_file, index=False)

    print(f"✅ Saved {len(chunk_df)} records → {output_file}")

print("🎉 All files saved successfully")
