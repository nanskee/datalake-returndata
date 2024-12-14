import os
import PyPDF2
import re
import csv
import pandas as pd
from flask import Flask, jsonify, request 
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


# Define directories for CSV, PDF, and TXT files
csv_dir = "data_lake/csv"
pdf_dir = "data_lake/pdf"
txt_dir = "data_lake/txt"

app = Flask(__name__)

# Retrieve PostgreSQL credentials from environment variables
pwd = os.environ['PGPASS']
uid = os.environ['PGIUD']

# PostgreSQL connection details
pg_host = "localhost"
pg_port = "5432"
pg_database = "adventureworks"

# PostgreSQL connection string using SQLAlchemy
postgresql_connection_string = f"postgresql://{uid}:{pwd}@{pg_host}:{pg_port}/{pg_database}"

# Create PostgreSQL engine using SQLAlchemy
engine = create_engine(postgresql_connection_string)

# Function to extract purchase info from PDF files
def extract_from_pdf(pdf_file):
    try:
        purchase_data = []
        with open(pdf_file, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text = page.extract_text()
                lines = text.split('\n')
                for line in lines:
                    if 'Purchase_ID' in line or 'Purchase_Date' in line:
                        continue
                    parts = re.split(r'\s+', line.strip())
                    if len(parts) >= 7:
                        try:
                            purchase_date = parts[0]  # First element should be Purchase_Date
                            total_amount = float(parts[-1])  # Last element should be Total_Amount
                            if purchase_date and total_amount > 0:
                                purchase_data.append({
                                    'purchase_date': purchase_date.lower(),  # Lowercase purchase_date
                                    'total_amount': total_amount,
                                    'source_file': os.path.basename(pdf_file).lower()  # Lowercase source_file
                                })
                        except (ValueError, IndexError):
                            continue
        return purchase_data
    except Exception as e:
        print(f"Error processing PDF file {pdf_file}: {e}")
        return []

# Function to extract purchase info from TXT files
def extract_from_txt(txt_file):
    try:
        purchase_data = []
        with open(txt_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            headers = lines[0].strip().split('\t')
            try:
                total_amount_idx = headers.index('total_amount')
                purchase_date_idx = headers.index('purchase_date')
            except ValueError:
                print(f"Required columns not found in {txt_file}")
                return []
            for line in lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) >= max(total_amount_idx, purchase_date_idx) + 1:
                    try:
                        purchase_data.append({
                            'purchase_date': parts[purchase_date_idx].lower(),  # Lowercase purchase_date
                            'total_amount': float(parts[total_amount_idx]),
                            'source_file': os.path.basename(txt_file).lower()  # Lowercase source_file
                        })
                    except (ValueError, IndexError):
                        continue
        return purchase_data
    except Exception as e:
        print(f"Error processing TXT file {txt_file}: {e}")
        return []

# Function to extract purchase info from CSV files
def extract_from_csv(csv_file):
    try:
        purchase_data = []
        df = pd.read_csv(csv_file)
        if 'Purchase_Date' in df.columns and 'Total_Amount' in df.columns:
            for _, row in df.iterrows():
                purchase_data.append({
                    'purchase_date': str(row['Purchase_Date']).lower(),
                    'total_amount': float(row['Total_Amount']),
                    'source_file': os.path.basename(csv_file).lower()
                })
        else:
            print(f"Required columns not found in {csv_file}")
            print(f"Available columns: {', '.join(df.columns)}")
        return purchase_data
    except Exception as e:
        print(f"Error processing CSV file {csv_file}: {e}")
        return []

# Function to process all files and return the combined purchase data
def process_all_files():
    all_purchase_data = []
    for directory, file_type, process_func in [
        (csv_dir, '.csv', extract_from_csv),
        (pdf_dir, '.pdf', extract_from_pdf),
        (txt_dir, '.txt', extract_from_txt)
    ]:
        if os.path.exists(directory):
            for file_name in os.listdir(directory):
                if file_name.endswith(file_type):
                    file_path = os.path.join(directory, file_name)
                    data = process_func(file_path)
                    all_purchase_data.extend(data)
                    print(f"Processed {file_type.upper()} file: {file_name}")
                    if data:
                        print(f"Found {len(data)} records in {file_name}")
    return all_purchase_data

# Function to insert purchase data into PostgreSQL
def insert_into_postgres(purchase_data):
    print(f"Current PostgreSQL user: {uid}")  # Add this line
    try:
        if purchase_data:
            df = pd.DataFrame(purchase_data)
            
            # Ensure data types are correct
            df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce')
            df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
            
            # Drop any rows with invalid data
            df = df.dropna()
            
            if not df.empty:
                # Add a timestamp column to track insertion time
                df['inserted_at'] = pd.Timestamp.now()
                
                # Use if_exists='append' to add new data to existing table
                df.to_sql('purchases', engine, if_exists='append', index=False)
                print(f"Successfully inserted {len(df)} records into PostgreSQL")
                return len(df)
            else:
                print("No valid data to insert after cleaning")
                return 0
        else:
            print("No data to insert into PostgreSQL")
            return 0
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error inserting data into PostgreSQL: {e}")
        return 0
    except Exception as e:
        print(f"Unexpected error inserting data into PostgreSQL: {e}")
        return 0

# API endpoint to get all purchase data with save option
@app.route('/api/purchases', methods=['GET'])
def get_purchases():
    purchase_data = process_all_files()
    
    # Check if save to PostgreSQL is requested via query parameter
    save_to_postgres = request.args.get('save', 'false').lower() == 'true'
    
    if save_to_postgres:
        inserted_count = insert_into_postgres(purchase_data)
        response_message = {
            "data": purchase_data,
            "message": f"Inserted {inserted_count} records into PostgreSQL"
        }
    else:
        response_message = {
            "data": purchase_data,
            "message": "Data retrieved but not saved to PostgreSQL. Add ?save=true to save."
        }
    
    return jsonify(response_message)

# API endpoint to get purchase summary with save option
@app.route('/api/summary', methods=['GET'])
def get_summary():
    try:
        purchase_data = process_all_files()
        if purchase_data:
            df = pd.DataFrame(purchase_data)
            total_purchases = len(df)
            total_amount = df['total_amount'].sum()
            avg_amount = df['total_amount'].mean()

            summary = {
                "total_purchases": total_purchases,
                "total_amount": total_amount,
                "average_amount": avg_amount
            }
            
            # Check if save to PostgreSQL is requested via query parameter
            save_to_postgres = request.args.get('save', 'false').lower() == 'true'
            
            if save_to_postgres:
                insert_into_postgres(purchase_data)
                summary["message"] = f"Inserted {total_purchases} records into PostgreSQL"
            else:
                summary["message"] = "Summary retrieved but not saved to PostgreSQL. Add ?save=true to save."
            
            return jsonify(summary)
        else:
            return jsonify({"message": "No purchase data found"}), 404
    except Exception as e:
        print(f"Error in /api/summary: {e}")
        return jsonify({"error": "An error occurred while generating the summary."}), 500

# Update index() method to show save instructions
@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Purchase Data</title>
    </head>
    <body>
        <h1>Purchase Data API 2</h1>
        <p>To save data to PostgreSQL, add ?save=true to the API calls</p>
        <button onclick="fetchPurchases(false)">Get Purchases</button>
        <button onclick="fetchPurchases(true)">Get & Save Purchases</button>
        <button onclick="fetchSummary()">Get Summary</button>
        <pre id="result"></pre>

        <script>
            function fetchPurchases(save) {
                const url = save ? '/api/purchases?save=true' : '/api/purchases';
                fetch(url)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Network response was not ok');
                        }
                        return response.json();
                    })
                    .then(data => {
                        document.getElementById('result').textContent = JSON.stringify(data, null, 2);
                    })
                    .catch(error => {
                        document.getElementById('result').textContent = 'Error: ' + error.message;
                    });
            }

            function fetchSummary() {
                fetch('/api/summary')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('result').textContent = JSON.stringify(data, null, 2);
                    });
            }
        </script>
    </body>
    </html>
    '''

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
