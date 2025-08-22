# Data Engineering Project-2: Used Car Prices ETL Pipeline

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline to process used car price data from multiple file formats (CSV, JSON, XML). The pipeline:

- Extracts data from CSV, JSON (newline-delimited), and XML files.
- Transforms the price field by converting it from Euro to INR and ensuring correct numeric types.
- Loads the transformed data into a consolidated CSV file.
- Logs each phase of the ETL workflow with timestamps for monitoring and debugging.

## Project Structure

- `etl_code.py` - Main ETL script.
- `log_progress.txt` - Log file capturing ETL job progress and timestamps.
- `transformed_data.csv` - Output file with cleaned and transformed data.
- `datasource.zip` (unzipped) - Source data files in CSV, JSON, and XML formats.

## How to Run

1. Ensure you have Python 3 installed.
2. Install required packages:
3. Run the ETL script
4. Check the output file `transformed_data.csv` for the consolidated data.
5. Review `log_progress.txt` to see ETL process logs.

## Key Code Components

- **Extraction:**
- Reads all CSV files using `pandas.read_csv()`.
- Reads JSON files as newline-delimited JSON using `pandas.read_json(lines=True)`.
- Parses XML files with `xml.etree.ElementTree` to extract car model, manufacture year, price, and fuel type.

- **Transformation:**
- Converts price values to numeric, coercing errors to NaN.
- Converts prices from Euro to INR by multiplying by 100.

- **Loading:**
- Saves the transformed data into `transformed_data.csv`.

- **Logging:**
- Writes log entries with timestamps for each ETL phase into `log_progress.txt`.



---





# used-car-prices-etl
