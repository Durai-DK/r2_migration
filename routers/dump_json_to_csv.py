import json
import os

file_path = "/Users/DK/Documents/_Transaction__202512111820.json"

def count_unique_customers(path):
    if not os.path.exists(path):
        print(f"Error: File not found at {path}")
        return

    try:
        with open(path, "r") as f:
            file_content = f.read()
            try:
                data = json.loads(file_content)
            except json.JSONDecodeError:
                print("Warning: Standard JSON load failed. Attempting to recover from malformed JSON...")
                # Attempt to find the array content
                start_index = file_content.find('[')
                end_index = file_content.rfind(']')
                if start_index != -1 and end_index != -1:
                    fixed_content = file_content[start_index:end_index+1]
                    data = json.loads(fixed_content)
                    print("Recovered JSON data from malformed file.")
                else:
                    raise

    except Exception as e:
        print(f"Error reading or decoding file: {e}")
        return

    records = []
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        # Check for common keys wrapping lists
        print(f"JSON root is a dictionary with keys: {list(data.keys())}")
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                print(f"Using list found in key: '{key}'")
                records = value
                break

        if not records:
             # If no obvious list found, treat the dict values as potentially the data if they are dicts
             # Or maybe it's just a single record?
             print("Could not automatically identify a list of records.")
             return

    print(f"Processing {len(records)} records...")

    unique_values = set()
    for item in records:
        if isinstance(item, dict):
            val = item.get("Customer_Number__c") or item.get("mobile_number")
            if val:
                unique_values.add(val)

    unique_count = len(unique_values)
    print(f"Unique Customer_Number__c count: {unique_count}")

    # Convert to list for chunking
    unique_list = list(unique_values)
    chunk_size = 1000000
    
    if not unique_list:
        print("No unique customers found to export.")
        return

    import csv
    
    base_name = os.path.splitext(os.path.basename(path))[0]
    output_dir = os.path.dirname(path)
    
    total_chunks = (len(unique_list) + chunk_size - 1) // chunk_size
    print(f"Exporting to {total_chunks} CSV files (approx {chunk_size} records per file)...")

    for i in range(total_chunks):
        chunk = unique_list[i * chunk_size : (i + 1) * chunk_size]
        output_filename = f"{base_name}_unique_customers_part_{i+1}.csv"
        output_path = os.path.join(output_dir, output_filename)
        
        try:
            with open(output_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['mobile_number'])  # Header
                for cust_id in chunk:
                    writer.writerow([cust_id])
            print(f"Saved chunk {i+1}/{total_chunks} to {output_path}")
        except Exception as e:
            print(f"Error writing to {output_path}: {e}")

if __name__ == "__main__":
    count_unique_customers(file_path)