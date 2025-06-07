# Flipkart Order Report ETL

This module uploads Flipkart order reports to a PostgreSQL table. The script
uses `rich` to provide an interactive prompt for selecting Excel files.

## Requirements
Install dependencies from the project root:

```bash
pip install -r ../../requirements.txt
```

## Environment Variables
Set the following variables to configure the database connection and defaults:

- `DB_HOST` (default: `localhost`)
- `DB_NAME` (default: `Shakedeal`)
- `DB_USER` (default: `postgres`)
- `DB_PASS` (default: `2705`)
- `ORDER_TABLE` (default: `flipkart_order_report`)
- `ORDER_REPORT_DIR` (optional default directory for reports)

## Usage
Run the ETL from the repository root using Python's `-m` flag:

```bash
python -m order_report_etl [path/to/report/dir]
```

The script will list available Excel files, let you choose which to upload,
and append the cleaned data to the specified PostgreSQL table.
