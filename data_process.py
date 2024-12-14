import os
import pandas as pd
import PyPDF2
import re

# Define directories
csv_dir = "data_lake/csv"
pdf_dir = "data_lake/pdf"
txt_dir = "data_lake/txt"

# Function to process PDF files and extract text
def process_pdf(pdf_file):
    try:
        with open(pdf_file, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page in reader.pages:
                text += page.extract_text()
            return text
    except Exception as e:
        print(f"Error processing PDF file {pdf_file}: {e}")
        return None

# Function to process CSV files using pandas
def process_csv(csv_file):
    try:
        df = pd.read_csv(csv_file)
        return df
    except Exception as e:
        print(f"Error processing CSV file {csv_file}: {e}")
        return None

# Function to process TXT files (simple text read)
def process_txt(txt_file):
    try:
        with open(txt_file, 'r') as file:
            text = file.read()
        return text
    except Exception as e:
        print(f"Error processing TXT file {txt_file}: {e}")
        return None

# Extract Purchase_ID and Total_Amount from structured data (CSV) and unstructured data (PDF, TXT)
def extract_purchase_id_and_total_amount_from_data(data):
    purchase_ids = []
    total_amounts = []

    if isinstance(data, pd.DataFrame):  # If data is a DataFrame (CSV data)
        if 'Purchase_ID' in data.columns and 'Total_Amount' in data.columns:
            purchase_ids = data['Purchase_ID'].tolist()
            total_amounts = data['Total_Amount'].tolist()
    elif isinstance(data, str):  # If data is a string (PDF or TXT content)
        # Regex to find Purchase_ID and Total_Amount (assuming they are numeric)
        purchase_ids = re.findall(r'Purchase_ID[:\s]*([\d]+)', data)
        total_amounts = re.findall(r'Total_Amount[:\s]*([\d.]+)', data)

    return purchase_ids, total_amounts

# Processing files in the directories
def process_data_lake():
    purchase_ids = []
    total_amounts = []

    # Process CSV files
    for file_name in os.listdir(csv_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(csv_dir, file_name)
            df = process_csv(file_path)
            if df is not None:
                print(f"Processed CSV file: {file_name}")
                # Extract Purchase_ID and Total_Amount from CSV data
                ids, amounts = extract_purchase_id_and_total_amount_from_data(df)
                purchase_ids.extend(ids)
                total_amounts.extend(amounts)

    # Process PDF files
    for file_name in os.listdir(pdf_dir):
        if file_name.endswith(".pdf"):
            file_path = os.path.join(pdf_dir, file_name)
            pdf_text = process_pdf(file_path)
            if pdf_text is not None:
                print(f"Processed PDF file: {file_name}")
                # Extract Purchase_ID and Total_Amount from PDF content
                ids, amounts = extract_purchase_id_and_total_amount_from_data(pdf_text)
                purchase_ids.extend(ids)
                total_amounts.extend(amounts)

    # Process TXT files
    for file_name in os.listdir(txt_dir):
        if file_name.endswith(".txt"):
            file_path = os.path.join(txt_dir, file_name)
            txt_text = process_txt(file_path)
            if txt_text is not None:
                print(f"Processed TXT file: {file_name}")
                # Extract Purchase_ID and Total_Amount from TXT content
                ids, amounts = extract_purchase_id_and_total_amount_from_data(txt_text)
                purchase_ids.extend(ids)
                total_amounts.extend(amounts)

    return purchase_ids, total_amounts

# Main processing
if __name__ == "__main__":
    # Ensure the directories exist
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)

    # Process data in the Data Lake
    purchase_ids, total_amounts = process_data_lake()

    # Display the extracted Purchase_IDs and Total_Amounts
    print("Extracted Purchase_IDs:", purchase_ids)
    print("Extracted Total_Amounts:", total_amounts)

