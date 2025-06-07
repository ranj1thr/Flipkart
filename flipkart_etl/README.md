# Flipkart ETL

This module provides a command-line interface to clean Flipkart Excel data and upload the results to a PostgreSQL database.

## Requirements
Install dependencies using:

```bash
pip install -r ../../requirements.txt
```

## Environment Variables
Set the following environment variables to configure the database connection and table name:

- `DB_HOST` (default: `localhost`)
- `DB_NAME` (default: `Shakedeal`)
- `DB_USER` (default: `postgres`)
- `DB_PASS` (default: `2705`)
- `TABLE_NAME` (default: `flipkart_scrap`)

## Usage
Run the ETL from the project root with Python's `-m` flag:

```bash
python -m flipkart_etl path/to/file.xlsx --sheet Sheet1
```

The script will clean the data, add an `insertion_date` column, and append the records to the configured PostgreSQL table.
