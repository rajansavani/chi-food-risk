import os
import time
import requests
import pandas as pd

from config import SOCRATA_ENDPOINT, SOCRATA_PAGE_SIZE, SOCRATA_APP_TOKEN, RAW_DATA_PATH


def fetch_one_page(offset: int) -> list[dict]:
    """
    Fetch a single page of results from the Socrata API:

        https://data.cityofchicago.org/resource/4ijn-s7e5.json
            ?$limit=50000
            &$offset=0
            &$order=inspection_id

    The Socrata server runs a query against their database, grabs
    up to 50,000 rows starting at the offset, and returns them as
    a JSON array. Use $order=inspection_id to make sure the
    pagination is stable (same row doesn't appear on two pages).
    """

    # build the query parameters
    params = {
        "$limit": SOCRATA_PAGE_SIZE,
        "$offset": offset,
        "$order": "inspection_id",
    }

    # include app token if we have it
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    # make the request or raise an error after 60s
    response = requests.get(
        SOCRATA_ENDPOINT,
        params=params,
        headers=headers,
        timeout=60,
    )

    response.raise_for_status()
    return response.json()


def ingest() -> pd.DataFrame:
    """
    This function calls fetch_one_page() repeatedly, each time increasing
    the offset by 50,000, until we get back an empty page (meaning we've
    reached the end of the dataset).
    """

    all_records = []
    offset = 0

    print("=" * 60)
    print("INGESTING DATA FROM CHICAGO OPEN DATA PORTAL")
    print("=" * 60)
    print(f"Endpoint:  {SOCRATA_ENDPOINT}")
    print(f"Page size: {SOCRATA_PAGE_SIZE:,} rows per request")
    print()

    while True:
        print(f"  Fetching rows {offset:,} to {offset + SOCRATA_PAGE_SIZE:,}...", end=" ")

        page = fetch_one_page(offset)

        print(f"received {len(page):,} rows")

        # if the page is empty, we've pulled everything
        if not page:
            break

        # add this page's rows to our running list
        all_records.extend(page)

        # move the offset forward for the next page
        offset += SOCRATA_PAGE_SIZE

        # avoid getting rate-limited hopefully
        time.sleep(1)

    print()
    print(f"Total records pulled: {len(all_records):,}")

    # convert the list of dictionaries into a pandas DataFrame
    # each dictionary becomes one row, and the keys become column names
    df = pd.DataFrame(all_records)

    return df


def save_raw_data(df: pd.DataFrame) -> None:
    # create the data/ directory if it doesn't exist yet
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)

    # save the raw data to a CSV file
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Saved to {RAW_DATA_PATH} ({len(df):,} rows, {len(df.columns)} columns)")


if __name__ == "__main__":
    df = ingest()
    save_raw_data(df)

    # print some basic info so we can sanity-check the pull
    print()
    print("Quick sanity check:")
    print(f"    Columns: {list(df.columns)}")
    print(f"    Date range: {df['inspection_date'].min()} to {df['inspection_date'].max()}")
    print(f"    Unique establishments (by license): {df['license_'].nunique():,}")
    print(f"    Result distribution:")
    print(df["results"].value_counts().to_string(header=False))
    print()
    print("Ingestion complete :D")