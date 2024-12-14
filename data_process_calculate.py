import os
import PyPDF2
import re
import csv
import pandas as pd

# Define directories
csv_dir = "data_lake/csv"
pdf_dir = "data_lake/pdf"
txt_dir = "data_lake/txt"

# Function to extract purchase info from PDF files
def extract_from_pdf(pdf_file):
    try:
        purchase_data = []
        with open(pdf_file, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text = page.extract_text()
                
                # Split text into lines and process each line
                lines = text.split('\n')
                for line in lines:
                    # Skip header line
                    if 'Purchase_ID' in line or 'Purchase_Date' in line:
                        continue
                    
                    # Pattern to match: date id customer product qty price amount
                    # Using more flexible pattern to account for varying formats
                    parts = re.split(r'\s+', line.strip())
                    if len(parts) >= 7:  # Ensure we have all required parts
                        try:
                            purchase_id = parts[1]  # Second element should be Purchase_ID
                            total_amount = float(parts[-1])  # Last element should be Total_Amount
                            if purchase_id.startswith('P') and total_amount > 0:
                                purchase_data.append({
                                    'Purchase_ID': purchase_id,
                                    'Total_Amount': total_amount,
                                    'Source_File': os.path.basename(pdf_file)
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
            
            # Find index of relevant columns
            try:
                total_amount_idx = headers.index('Total_Amount')
                product_id_idx = headers.index('Product_ID')
            except ValueError:
                print(f"Required columns not found in {txt_file}")
                return []
            
            # Process each line
            for line in lines[1:]:  # Skip header
                parts = line.strip().split('\t')
                if len(parts) >= max(total_amount_idx, product_id_idx) + 1:
                    try:
                        purchase_data.append({
                            'Purchase_ID': parts[product_id_idx],  # Using Product_ID as Purchase_ID
                            'Total_Amount': float(parts[total_amount_idx]),
                            'Source_File': os.path.basename(txt_file)
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
        
        # Check for required columns
        if 'Purchase_ID' in df.columns and 'Total_Amount' in df.columns:
            for _, row in df.iterrows():
                purchase_data.append({
                    'Purchase_ID': str(row['Purchase_ID']),
                    'Total_Amount': float(row['Total_Amount']),
                    'Source_File': os.path.basename(csv_file)
                })
        else:
            print(f"Required columns not found in {csv_file}")
            print(f"Available columns: {', '.join(df.columns)}")
            
        return purchase_data
    except Exception as e:
        print(f"Error processing CSV file {csv_file}: {e}")
        return []

def process_all_files():
    all_purchase_data = []
    
    # Process each type of file
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

def save_results(purchase_data, output_file='purchase_summary.csv'):
    if purchase_data:
        df = pd.DataFrame(purchase_data)
        # Sort by Purchase_ID
        df = df.sort_values('Purchase_ID')
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        # Display summary statistics
        total_purchases = len(df)
        total_amount = df['Total_Amount'].sum()
        avg_amount = df['Total_Amount'].mean()
        
        print(f"\nSummary Statistics:")
        print(f"Total number of purchases: {total_purchases}")
        print(f"Total amount: ${total_amount:,.2f}")
        print(f"Average amount per purchase: ${avg_amount:,.2f}")
        
        # Display summary by file type
        print("\nPurchases by file type:")
        file_summary = df.groupby('Source_File').agg({
            'Purchase_ID': 'count',
            'Total_Amount': 'sum'
        }).reset_index()
        print(file_summary)
        
        # Display first few records
        print("\nFirst few records:")
        print(df.head())
    else:
        print("No purchase data found")

if __name__ == "__main__":
    # Create directories if they don't exist
    for directory in [csv_dir, pdf_dir, txt_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Process all files and get purchase data
    purchase_data = process_all_files()
    
    # Save and display results
    save_results(purchase_data)