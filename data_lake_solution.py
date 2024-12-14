import os
import PyPDF2
import re
import csv
import pandas as pd
import pyodbc
from flask import Flask, jsonify, request 
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename


# Define directories for CSV, PDF, and TXT files
csv_dir = "data_lake/csv"
pdf_dir = "data_lake/pdf"
txt_dir = "data_lake/txt"


app = Flask(__name__)

# Retrieve SQL Server credentials from environment variables
pwd = os.environ['PGPASS']
uid = os.environ['PGIUD']

# SQL Server connection details
sql_server_driver = "{SQL Server Native Client 11.0}"
sql_server = "DESKTOP-HK27CB8\\SQLEXPRESS"
sql_database = "AdventureWorks2019"

# SQL Server connection string using SQLAlchemy
sqlserver_connection_string = pyodbc.connect(
            f'DRIVER={sql_server_driver};SERVER={sql_server};DATABASE={sql_database};UID={uid};PWD={pwd}'
        )

# Create SQL Server engine using SQLAlchemy
engine = create_engine(f'mssql+pyodbc://{uid}:{pwd}@{sql_server}/{sql_database}?driver=SQL+Server+Native+Client+11.0')

# Define allowed file extensions for uploads
ALLOWED_EXTENSIONS = {'.csv', '.pdf', '.txt'}

def allowed_file(filename):
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)

# Function to save uploaded file to the correct directory
def save_file_to_datalake(file, filename):
    file_extension = os.path.splitext(filename)[1].lower()
    if file_extension == '.csv':
        directory = csv_dir
    elif file_extension == '.pdf':
        directory = pdf_dir
    elif file_extension == '.txt':
        directory = txt_dir
    else:
        return None  # Unsupported file type
    
    # Ensure directory exists
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)
    file.save(file_path)
    return file_path

def extract_from_pdf(pdf_file):
    try:
        return_data = []
        with open(pdf_file, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text = page.extract_text()
                lines = text.split('\n')
                for line in lines:
                    parts = re.split(r'\s+', line.strip())
                    if len(parts) >= 4:
                        try:
                            return_date = parts[0]
                            territory_key = parts[1]
                            product_key = parts[2]
                            return_quantity = int(parts[3])

                            # Validate date format
                            try:
                                return_date = pd.to_datetime(return_date, format='%m/%d/%Y').strftime('%Y-%m-%d')
                            except ValueError:
                                return_date = None  # Mark invalid dates as None

                            if return_date and return_quantity >= 0:
                                return_data.append({
                                    'return_date': return_date,
                                    'territory_key': territory_key,
                                    'product_key': product_key,
                                    'return_quantity': return_quantity,
                                    'source_file': os.path.basename(pdf_file)
                                })
                        except (ValueError, IndexError):
                            continue
        return return_data
    except Exception as e:
        print(f"Error processing PDF file {pdf_file}: {e}")
        return []

def extract_from_txt(txt_file):
    try:
        return_data = []
        with open(txt_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line_num, line in enumerate(lines[1:], 2):  # Skip header
                try:
                    parts = [part for part in line.split() if part.strip()]
                    if len(parts) >= 4:
                        return_date = parts[0]
                        territory_key = parts[1]
                        product_key = parts[2]
                        return_quantity = int(parts[3])

                        # Validate date format
                        try:
                            return_date = pd.to_datetime(return_date, format='%m/%d/%Y').strftime('%Y-%m-%d')
                        except ValueError:
                            return_date = None

                        if return_date and return_quantity >= 0:
                            return_data.append({
                                'return_date': return_date,
                                'territory_key': territory_key,
                                'product_key': product_key,
                                'return_quantity': return_quantity,
                                'source_file': os.path.basename(txt_file)
                            })
                except (ValueError, IndexError):
                    continue
        return return_data
    except Exception as e:
        print(f"Error processing TXT file {txt_file}: {e}")
        return []

def extract_from_csv(csv_file):
    try:
        return_data = []
        df = pd.read_csv(csv_file)
        if {'ReturnDate', 'TerritoryKey', 'ProductKey', 'ReturnQuantity'}.issubset(df.columns):
            for _, row in df.iterrows():
                try:
                    return_date = pd.to_datetime(row['ReturnDate'], errors='coerce').strftime('%Y-%m-%d')
                    territory_key = str(row['TerritoryKey'])
                    product_key = str(row['ProductKey'])
                    return_quantity = int(row['ReturnQuantity'])

                    if return_date and return_quantity >= 0:
                        return_data.append({
                            'return_date': return_date,
                            'territory_key': territory_key,
                            'product_key': product_key,
                            'return_quantity': return_quantity,
                            'source_file': os.path.basename(csv_file)
                        })
                except (ValueError, KeyError):
                    continue
        else:
            print(f"Required columns not found in {csv_file}")
        return return_data
    except Exception as e:
        print(f"Error processing CSV file {csv_file}: {e}")
        return []

# Function to process all files and return the combined return data
def process_all_files():
    all_return_data = []
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
                    all_return_data.extend(data)
                    print(f"Processed {file_type.upper()} file: {file_name}")
                    if data:
                        print(f"Found {len(data)} records in {file_name}")
    return all_return_data

# Function to insert return data into SQL Server
def insert_into_sqlserver(return_data):
    print("\n=== Starting SQL Server Insert Process ===")
    print(f"Records to process: {len(return_data)}")
    
    try:
        if not return_data:
            print("No data to insert")
            return 0
            
        # Create DataFrame
        df = pd.DataFrame(return_data)
        print(f"\nInitial DataFrame shape: {df.shape}")
        print("\nColumn data types:")
        print(df.dtypes)
        
        # Print sample data before conversion
        print("\nSample data before conversion:")
        print(df.head())
        
        # Convert return_date
        print("\nConverting dates...")
        df['return_date'] = pd.to_datetime(df['return_date'], errors='coerce')
        invalid_dates = df[df['return_date'].isna()]
        if not invalid_dates.empty:
            print(f"Found {len(invalid_dates)} invalid dates:")
            print(invalid_dates)
        
        # Drop invalid rows
        original_len = len(df)
        df = df.dropna()
        dropped_rows = original_len - len(df)
        
        if dropped_rows > 0:
            print(f"\nDropped {dropped_rows} invalid rows")
            print(f"Remaining valid rows: {len(df)}")
        
        if df.empty:
            print("No valid data remaining after cleanup")
            return 0
        
        # Add timestamp
        df['inserted_at'] = pd.Timestamp.now()
        
        # Test database connection
        print("\nTesting database connection...")
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                print("Database connection successful")
        except Exception as e:
            print(f"Database connection failed: {str(e)}")
            raise  # Re-raise the exception to be caught by outer try-except
        
        # Insert data
        print("\nInserting data into SQL Server...")
        df.to_sql('Returns', engine, if_exists='append', index=False)
        print(f"Successfully inserted {len(df)} records")
        return len(df)
        
    except Exception as e:
        print(f"Error during insertion: {str(e)}")
        return 0
    finally:
        print("=== SQL Server Insert Process Completed ===")

# API endpoint to get all return data with save option
@app.route('/api/returns', methods=['GET'])
def get_returns():
    return_data = process_all_files()
    
    # Check if save to SQL Server is requested via query parameter
    save_to_sqlserver = request.args.get('save', 'false').lower() == 'true'
    
    if save_to_sqlserver:
        inserted_count = insert_into_sqlserver(return_data)
        response_message = {
            "data": return_data,
            "message": f"Inserted {inserted_count} records into SQL Server"
        }
    else:
        response_message = {
            "data": return_data,
            "message": "Data retrieved but not saved to SQL Server. Click Get and Returns to save."
        }
    
    return jsonify(response_message)

# API endpoint for file upload
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = save_file_to_datalake(file, filename)
        if file_path:
            return jsonify({
                "message": f"File successfully uploaded to {file_path}",
                "file_name": filename,
                "file_path": file_path
            }), 200
        else:
            return jsonify({"error": "Unsupported file type"}), 400
    else:
        return jsonify({"error": "Invalid file or unsupported extension"}), 400

# Update index() method to show save instructions
@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Return Data API</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 0;
                background-color: #f4f4f9;
                color: #333;
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
            }
            .container {
                background: #fff;
                padding: 20px 30px;
                border-radius: 10px;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
                max-width: 600px;
                text-align: center;
            }
            h1 {
                margin-bottom: 20px;
                font-size: 24px;
                color: #444;
            }
            p {
                margin-bottom: 20px;
                color: #666;
            }
            button, input[type="file"] {
                display: inline-block;
                padding: 10px 15px;
                margin: 10px 5px;
                font-size: 14px;
                border-radius: 5px;
                border: none;
                cursor: pointer;
                transition: background-color 0.3s;
            }
            button {
                background-color: #007BFF;
                color: white;
            }
            button:hover {
                background-color: #0056b3;
            }
            .save-btn {
                background-color: #28a745; /* Warna hijau */
                color: white;
            }
            .save-btn:hover {
                background-color: #218838;
            }
            input[type="file"] {
                background: #eee;
                color: #333;
                cursor: pointer;
            }
            input[type="file"]:hover {
                background: #ddd;
            }
            pre {
                background: #f8f8f8;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 5px;
                overflow-x: auto;
                text-align: left;
                font-size: 14px;
            }
            form {
                margin-top: 20px;
            }
            .button-group {
                display: flex;
                justify-content: center;
                flex-wrap: wrap;
                gap: 10px;
                margin-bottom: 20px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Return Data API</h1>
            <p>To save data to SQL Server, click "Get & Save Return".</p>
            <div class="button-group">
                <button onclick="fetchReturns(false)">Get Return</button>
                <button class="save-btn" onclick="fetchReturns(true)">Get & Save Returns</button>
            </div>
            
            <form id="uploadForm" enctype="multipart/form-data">
                <h2>Upload a File</h2>
                <input type="file" id="fileInput" name="file" />
                <button type="button" onclick="uploadFile()">Upload File</button>
            </form>
            <pre id="result"></pre>
        </div>

        <script>
            function fetchReturns(save) {
                const url = save ? '/api/returns?save=true' : '/api/returns';
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

            function uploadFile() {
                const fileInput = document.getElementById('fileInput');
                const file = fileInput.files[0];

                if (!file) {
                    document.getElementById('result').textContent = 'Please select a file before uploading.';
                    return;
                }

                const formData = new FormData();
                formData.append('file', file);

                fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    document.getElementById('result').textContent = JSON.stringify(data, null, 2);
                })
                .catch(error => {
                    document.getElementById('result').textContent = 'Error: ' + error.message;
                });
            }
        </script>
    </body>
    </html>
    '''

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
