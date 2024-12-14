import os
import PyPDF2
import re
import csv

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

# Function to process TXT files
def process_txt(txt_file):
    try:
        with open(txt_file, 'r', encoding='utf-8') as file:    # Fixed missing colon here
            text = file.read()
        return text
    except Exception as e:
        print(f"Error processing TXT file {txt_file}: {e}")
        return None

# Function to process CSV files
def process_csv(csv_file):
    try:
        quality_comments = []
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'Comment' in row:
                    comment = row['Comment'].strip()
                    if 'quality' in comment.lower():
                        quality_comments.append(comment)
        return quality_comments
    except Exception as e:
        print(f"Error processing CSV file {csv_file}: {e}")
        return []

# Improved function to find sentences containing the word 'quality'
def find_sentences_with_quality(text):
    if not text:
        return []
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
    quality_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        if sentence and 'quality' in sentence.lower():
            quality_sentences.append(sentence)
    return quality_sentences

def process_data_lake():
    quality_sentences = []
    
    # Process each type of file
    for directory, process_func in [
        (csv_dir, lambda f: process_csv(f)),
        (pdf_dir, lambda f: find_sentences_with_quality(process_pdf(f))),
        (txt_dir, lambda f: find_sentences_with_quality(process_txt(f)))
    ]:
        if os.path.exists(directory):
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                results = process_func(file_path)
                if results:
                    quality_sentences.extend(results)
                    print(f"Found quality-related content in: {file_name}")
    
    return quality_sentences

if __name__ == "__main__":
    # Ensure directories exist
    for directory in [csv_dir, pdf_dir, txt_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Process data and display results
    quality_sentences = process_data_lake()
    
    print("\nSentences containing 'quality':")
    for sentence in quality_sentences:
        print(f"- {sentence}")