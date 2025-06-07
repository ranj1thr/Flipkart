import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from rich.console import Console
from rich.tree import Tree
from rich.prompt import Prompt
from pathlib import Path

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "Shakedeal")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "2705")
TABLE_NAME = os.environ.get("ORDER_TABLE", "flipkart_order_report")
DEFAULT_REPORT_DIR = os.environ.get("ORDER_REPORT_DIR", ".")

console = Console(soft_wrap=True, force_terminal=True)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def format_fields(df: pd.DataFrame) -> pd.DataFrame:
    if 'sku' in df.columns:
        df['sku'] = df['sku'].astype(str).str.replace('SKU:', '', case=False).str.replace('"', '').str.strip()
    if 'product_title' in df.columns:
        df['product_title'] = df['product_title'].astype(str).str.strip('"').str.strip()
    if 'order_item_id' in df.columns:
        df['order_item_id'] = df['order_item_id'].astype(str).str.replace('OI:', '').str.strip()
    if 'delivery_tracking_id' in df.columns:
        df['delivery_tracking_id'] = df['delivery_tracking_id'].astype(str).str.replace('DTr:', '').str.strip()
    if 'procurement_dispatch_sla' in df.columns:
        df['procurement_dispatch_sla'] = pd.to_numeric(df['procurement_dispatch_sla'], errors='coerce')
    return df

def create_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    df['unique_key'] = (
        df['order_item_id'].astype(str) + '-' +
        df['order_id'].astype(str) + '-' +
        df['order_item_status'].astype(str) + '-' +
        df['delivery_tracking_id'].astype(str)
    )
    return df

def display_folder_structure(base_path: Path):
    file_map = {}
    counter = 1
    tree = Tree("[FOLDER] Available Files and Folders")

    def process_directory(path: Path, tree_node: Tree):
        nonlocal counter
        for item in sorted(path.iterdir()):
            if item.is_dir():
                branch = tree_node.add(f"[DIR] {item.name}")
                process_directory(item, branch)
            elif item.is_file() and item.suffix.lower() in ('.xlsx', '.xls'):
                file_map[counter] = str(item)
                tree_node.add(f"[FILE {counter}] {item.name}")
                counter += 1

    process_directory(base_path, tree)
    console.print(tree)
    return file_map

def select_sheet(file_path: str) -> str:
    xls = pd.ExcelFile(file_path)
    if 'Orders' in xls.sheet_names:
        return 'Orders'
    if len(xls.sheet_names) == 1:
        return xls.sheet_names[0]
    console.print("\n[yellow]Available sheets:[/yellow]")
    for idx, sheet in enumerate(xls.sheet_names, 1):
        console.print(f"[cyan]{idx}.[/cyan] {sheet}")
    choice = int(Prompt.ask("\nSelect sheet number", default="1"))
    return xls.sheet_names[choice - 1]

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_columns = [
        'order_date',
        'order_approval_date',
        'order_cancellation_date',
        'order_return_approval_date',
        'dispatch_after_date',
        'dispatch_by_date',
        'order_ready_for_dispatch_on_date',
        'dispatched_date',
        'deliver_by_date',
        'order_delivery_date',
        'service_by_date',
    ]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    return df

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Upload Flipkart order reports to PostgreSQL")
    parser.add_argument(
        "directory",
        nargs="?",
        default=DEFAULT_REPORT_DIR,
        help="Directory containing Excel reports"
    )
    args = parser.parse_args()

    base_path = Path(args.directory)
    file_map = display_folder_structure(base_path)
    if not file_map:
        console.print("[red]No Excel files found.[/red]")
        return

    while True:
        try:
            console.print("\n[cyan]Enter file numbers to process (comma-separated) or 0 to exit:[/cyan]")
            console.print("[cyan]Example: 1,3,4 to process multiple files[/cyan]")
            choice = Prompt.ask("\nEnter selection", default="0")
            if choice == "0":
                break
            try:
                file_numbers = [int(x.strip()) for x in choice.split(",")]
            except ValueError:
                console.print("[red]Invalid input. Please enter numbers separated by commas.[/red]")
                continue
            invalid_numbers = [num for num in file_numbers if num not in file_map]
            if invalid_numbers:
                console.print(f"[red]Invalid file numbers: {invalid_numbers}[/red]")
                continue
            connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
            engine = create_engine(connection_string)
            try:
                try:
                    existing_keys_df = pd.read_sql(f'SELECT unique_key FROM {TABLE_NAME};', engine)
                    existing_keys = set(existing_keys_df['unique_key'].dropna().unique())
                except Exception as e:
                    console.print(f"[yellow]Could not fetch existing keys: {e}. Will proceed without duplicate checking.[/yellow]")
                    existing_keys = set()
                for file_num in file_numbers:
                    excel_file_path = file_map[file_num]
                    console.print(f"\n[green]Processing file: {Path(excel_file_path).name}[/green]")
                    try:
                        sheet_name = select_sheet(excel_file_path)
                        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
                        df = normalize_columns(df)
                        df = format_fields(df)
                        df = convert_date_columns(df)
                        df['source_file'] = os.path.basename(excel_file_path)
                        df = create_unique_key(df)
                        console.print(f"[green]Loaded {len(df)} rows from file.[/green]")
                        before = len(df)
                        df = df[~df['unique_key'].isin(existing_keys)]
                        filtered = before - len(df)
                        if filtered > 0:
                            console.print(f"[yellow]Filtered {filtered} duplicate rows based on 'unique_key'.[/yellow]")
                        if not df.empty:
                            dtype_map = {col: Date() for col in [
                                'order_date',
                                'order_approval_date',
                                'order_cancellation_date',
                                'order_return_approval_date',
                                'dispatch_after_date',
                                'dispatch_by_date',
                                'order_ready_for_dispatch_on_date',
                                'dispatched_date',
                                'deliver_by_date',
                                'order_delivery_date',
                                'service_by_date',
                            ] if col in df.columns}
                            df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, dtype=dtype_map)
                            console.print(f"[green]Uploaded {len(df)} new rows to '{TABLE_NAME}'.[/green]")
                            existing_keys.update(set(df['unique_key']))
                        else:
                            console.print("[cyan]No new rows to upload from this file.[/cyan]")
                    except Exception as e:
                        console.print(f"[red]Error processing file {Path(excel_file_path).name}: {e}[/red]")
                        continue
            finally:
                engine.dispose()
            console.print("\n[cyan]Would you like to process more files? (y/n)[/cyan]")
            if Prompt.ask("Continue?", choices=["y", "n"], default="n") == "n":
                break
        except Exception as error:
            console.print(f"[red]Error: {error}[/red]")

if __name__ == "__main__":
    main()
