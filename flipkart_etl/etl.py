import os
import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import datetime
import argparse

# === Database connection details via environment variables ===
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Shakedeal")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "2705")
TABLE_NAME = os.environ.get("TABLE_NAME", "flipkart_scrap")

# === Column renaming and dropping dictionaries ===
RENAME_DICT = {
    "TextHighlight": "Title",
    "TextHighlight 2": "SKU",
    "TextHighlight 3": "Category",
    "styles__SellingPriceWrapper-sc-1n6ywsa-2": "Listing Price",
    "styles__TbodyCell-dsgsck-4": "Benchmark Price",
    "styles__FinalPriceWrapper-sc-dk1mu2-0": "Final Price",
    "styles__RightAlignWrapper-sc-g11inc-0": "MRP",
    "styles__FlexRow-sc-itsxp5-4": "Stock",
    "styles__DoHWrapper-sc-itsxp5-6": "DOH",
    "styles__TbodyCell-dsgsck-4 2": "Fulfillment Type",
    "styles__RightAlignWrapper-sc-g11inc-0 2": "SLA",
    "styles__StatusValue-y9chu4-1": "LQS",
    "styles__ClickableContainer-sc-16isc7k-2 href": "Listing Link",
    "styles__ReturnsCellContainer-sc-890y2z-17": "Returns"
}

DROP_COLUMNS = [
    "styles__ProductIcon-sc-p666j7-4 src",
    "styles__ReturnsContainer-sc-1qs5x67-4",
    "styles__RedText-sc-1qs5x67-5",
    "styles__ReturnsContainer-sc-1qs5x67-4 href",
    "styles__TbodyCell-dsgsck-4 3"
]

def extract_fsn(url: str):
    """Parse the FSN (Flipkart Serial Number) from a URL."""
    match = re.search(r'[\?&]pid=([A-Z0-9]+)', str(url))
    return match.group(1) if match else None

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and format the Flipkart scrap data."""
    df = df.copy()
    df.rename(columns=RENAME_DICT, inplace=True)
    df.drop(columns=DROP_COLUMNS, inplace=True, errors="ignore")

    text_columns = ["Title", "SKU", "Category", "Fulfillment Type", "LQS"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    num_columns = ["Listing Price", "Benchmark Price", "Final Price", "MRP", "Stock", "Returns"]
    for col in num_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"[^\d.]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Stock" in df.columns:
        df["Stock"] = df["Stock"].fillna(0)
    if "Benchmark Price" in df.columns:
        median_benchmark = df["Benchmark Price"].median()
        df["Benchmark Price"] = df["Benchmark Price"].fillna(median_benchmark)
    if "Returns" in df.columns:
        df["Returns"] = df["Returns"].fillna(0)

    df.drop_duplicates(inplace=True)

    if "Listing Link" in df.columns:
        df = df[df["Listing Link"].astype(str).str.startswith(("http", "https"))]
        df["FSN"] = df["Listing Link"].apply(extract_fsn)

    if "Fulfillment Type" in df.columns:
        df["Fulfillment Type"] = df["Fulfillment Type"].replace({
            "Flipkart and Seller Only": "Flipkart & Seller Only"
        })
    if "LQS" in df.columns:
        df["LQS"] = df["LQS"].str.capitalize()

    return df

def upload_to_postgres(df: pd.DataFrame):
    """Upload the cleaned dataframe to PostgreSQL."""
    df = df.applymap(lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"✅ Data appended to table '{TABLE_NAME}' in PostgreSQL.")

def main():
    parser = argparse.ArgumentParser(description="Clean Flipkart Excel data and upload to PostgreSQL")
    parser.add_argument("excel_path", help="Path to the input Excel file")
    parser.add_argument("--sheet", default="Recovered_Sheet1", help="Excel sheet name")
    args = parser.parse_args()

    df_raw = pd.read_excel(args.excel_path, sheet_name=args.sheet, engine="openpyxl")
    df_cleaned = clean_data(df_raw)
    df_cleaned['insertion_date'] = datetime.now().date()
    upload_to_postgres(df_cleaned)

if __name__ == "__main__":
    main()
