import requests, time, pandas as pd


TOKEN = ("eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJpc3MiOiAicG9zLWJ1Y2tldC1hcGkiLCAiYXVkIjogInBvcy1idWNrZXQtY2xpZW50I"
         "iwgImlhdCI6IDE3NzMyMDkzNzQxOTMsICJleHAiOiAxNzczMjk1Nzc0MTkzLCAianRpIjogImFhMjUwMjU2LWY0YjgtNDVhZi1iNjhlLTNjNmF"
         "mYjkyYTQzYSIsICJ0eXBlIjogImFjY2VzcyIsICJ0b2tlbl9pZCI6ICJwb3MjdHJhbnNAcjIqYXBpIURLIn0.GIyoxleUpHEGqeJf6AdN_xTbL"
         "oyd8Mwqf0Z50PPKtYc")


REQUEST_DELAY = 3
INPUT_FILE = "imei_list.xlsx"
OUTPUT_FILE = "imei_status_output.xlsx"


def fetch_status(imei):
    try:
        url = f"https://invoice-history.poorvika.in/api/pos-bucket/fetch-records?imei={imei}"

        response = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"})

        return response.status_code

    except Exception as e:
        print(f"Error for IMEI {imei}: {e}")
        return "ERROR"


def process_excel(input_file, output_file):

    df = pd.read_excel(input_file)

    # Add new column if not exists
    if "status" not in df.columns:
        df["status"] = ""

    count_200 = 0
    count_404 = 0

    max_len = len(df)

    for i in range(max_len):

        imei = df.loc[i, "imei"]

        if pd.isna(imei):
            continue

        status = fetch_status(imei)

        df.loc[i, "status"] = status

        if status == 200:
            count_200 += 1
        elif status == 404:
            count_404 += 1

        print(f"Row {i+1}/{max_len} | IMEI: {imei} -> {status}")

        # Prevent R2 throttling
        time.sleep(REQUEST_DELAY)

    df.to_excel(output_file, index=False)

    print("\n------ SUMMARY ------")
    print(f"200 Count : {count_200}")
    print(f"404 Count : {count_404}")
    print(f"Total Rows: {max_len}")
    print("Output saved to:", output_file)


process_excel(INPUT_FILE, OUTPUT_FILE)
