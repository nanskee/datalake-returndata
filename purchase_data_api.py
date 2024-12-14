# file: purchase_data_api.py

import os
import PyPDF2
import re
import csv
import pandas as pd
from typing import List, Dict, Optional, Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime

# Define data models
class PurchaseData(BaseModel):
    Purchase_ID: str
    Total_Amount: float
    Source_File: str

class PurchaseStats(BaseModel):
    total_purchases: int
    total_amount: float
    average_amount: float
    file_summary: List[Dict]

class DataProcessor:
    def __init__(self, csv_dir="data_lake/csv", pdf_dir="data_lake/pdf", txt_dir="data_lake/txt"):
        self.csv_dir = csv_dir
        self.pdf_dir = pdf_dir
        self.txt_dir = txt_dir
        self._cached_data = None
        self._last_update = None
        self._cache_duration = 300  # 5 minutes cache

    def _extract_from_pdf(self, pdf_file: str) -> List[Dict]:
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
                                purchase_id = parts[1]
                                total_amount = float(parts[-1])
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

    def _extract_from_txt(self, txt_file: str) -> List[Dict]:
        try:
            purchase_data = []
            with open(txt_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                headers = lines[0].strip().split('\t')
                try:
                    total_amount_idx = headers.index('Total_Amount')
                    product_id_idx = headers.index('Product_ID')
                except ValueError:
                    return []
                
                for line in lines[1:]:
                    parts = line.strip().split('\t')
                    if len(parts) >= max(total_amount_idx, product_id_idx) + 1:
                        try:
                            purchase_data.append({
                                'Purchase_ID': parts[product_id_idx],
                                'Total_Amount': float(parts[total_amount_idx]),
                                'Source_File': os.path.basename(txt_file)
                            })
                        except (ValueError, IndexError):
                            continue
            return purchase_data
        except Exception as e:
            print(f"Error processing TXT file {txt_file}: {e}")
            return []

    def _extract_from_csv(self, csv_file: str) -> List[Dict]:
        try:
            purchase_data = []
            df = pd.read_csv(csv_file)
            if 'Purchase_ID' in df.columns and 'Total_Amount' in df.columns:
                for _, row in df.iterrows():
                    purchase_data.append({
                        'Purchase_ID': str(row['Purchase_ID']),
                        'Total_Amount': float(row['Total_Amount']),
                        'Source_File': os.path.basename(csv_file)
                    })
            return purchase_data
        except Exception as e:
            print(f"Error processing CSV file {csv_file}: {e}")
            return []

    def _is_cache_valid(self) -> bool:
        if not self._cached_data or not self._last_update:
            return False
        elapsed = (datetime.now() - self._last_update).total_seconds()
        return elapsed < self._cache_duration

    def get_all_purchase_data(self, use_cache: bool = True) -> List[PurchaseData]:
        """
        Get all purchase data from all sources.
        Args:
            use_cache (bool): Whether to use cached data if available
        Returns:
            List of PurchaseData objects
        """
        if use_cache and self._is_cache_valid():
            return self._cached_data

        all_purchase_data = []
        
        for directory, file_type, extract_func in [
            (self.csv_dir, '.csv', self._extract_from_csv),
            (self.pdf_dir, '.pdf', self._extract_from_pdf),
            (self.txt_dir, '.txt', self._extract_from_txt)
        ]:
            if os.path.exists(directory):
                for file_name in os.listdir(directory):
                    if file_name.endswith(file_type):
                        file_path = os.path.join(directory, file_name)
                        all_purchase_data.extend(extract_func(file_path))

        self._cached_data = all_purchase_data
        self._last_update = datetime.now()
        return all_purchase_data

    def get_purchase_by_id(self, purchase_id: str) -> Optional[PurchaseData]:
        """
        Get purchase data by Purchase ID.
        Args:
            purchase_id (str): Purchase ID to search for
        Returns:
            PurchaseData object if found, None otherwise
        """
        data = self.get_all_purchase_data()
        for purchase in data:
            if purchase['Purchase_ID'] == purchase_id:
                return purchase
        return None

    def get_purchases_by_amount_range(self, min_amount: float, max_amount: float) -> List[PurchaseData]:
        """
        Get purchases within a specified amount range.
        Args:
            min_amount (float): Minimum amount
            max_amount (float): Maximum amount
        Returns:
            List of PurchaseData objects within the range
        """
        data = self.get_all_purchase_data()
        return [p for p in data if min_amount <= p['Total_Amount'] <= max_amount]

    def get_purchase_statistics(self) -> PurchaseStats:
        """
        Get statistical summary of all purchases.
        Returns:
            PurchaseStats object containing summary statistics
        """
        data = self.get_all_purchase_data()
        if not data:
            return PurchaseStats(
                total_purchases=0,
                total_amount=0,
                average_amount=0,
                file_summary=[]
            )

        df = pd.DataFrame(data)
        file_summary = df.groupby('Source_File').agg({
            'Purchase_ID': 'count',
            'Total_Amount': 'sum'
        }).reset_index().to_dict('records')

        return PurchaseStats(
            total_purchases=len(data),
            total_amount=df['Total_Amount'].sum(),
            average_amount=df['Total_Amount'].mean(),
            file_summary=file_summary
        )

# Create FastAPI instance
app = FastAPI(title="Purchase Data API")

# Initialize data processor
data_processor = DataProcessor()

@app.get("/purchases/", response_model=List[PurchaseData])
async def get_all_purchases():
    """Get all purchase data"""
    return data_processor.get_all_purchase_data()

@app.get("/purchases/{purchase_id}", response_model=Optional[PurchaseData])
async def get_purchase(purchase_id: str):
    """Get purchase by ID"""
    purchase = data_processor.get_purchase_by_id(purchase_id)
    if not purchase:
        raise HTTPException(status_code=404, detail="Purchase not found")
    return purchase

@app.get("/purchases/range/", response_model=List[PurchaseData])
async def get_purchases_by_range(min_amount: float, max_amount: float):
    """Get purchases within amount range"""
    return data_processor.get_purchases_by_amount_range(min_amount, max_amount)

@app.get("/statistics/", response_model=PurchaseStats)
async def get_statistics():
    """Get purchase statistics"""
    return data_processor.get_purchase_statistics()

# Example usage of the Python functions
if __name__ == "__main__":
    # Initialize processor
    processor = DataProcessor()
    
    # Example 1: Get all purchase data
    print("\nAll Purchases:")
    all_data = processor.get_all_purchase_data()
    print(f"Found {len(all_data)} purchases")
    
    # Example 2: Get specific purchase
    purchase_id = "P0001"
    print(f"\nLooking for purchase {purchase_id}:")
    purchase = processor.get_purchase_by_id(purchase_id)
    print(purchase if purchase else "Not found")
    
    # Example 3: Get purchases in range
    print("\nPurchases between $20000 and $50000:")
    range_purchases = processor.get_purchases_by_amount_range(20000, 50000)
    print(f"Found {len(range_purchases)} purchases in range")
    
    # Example 4: Get statistics
    print("\nPurchase Statistics:")
    stats = processor.get_purchase_statistics()
    print(f"Total Purchases: {stats.total_purchases}")
    print(f"Total Amount: ${stats.total_amount:,.2f}")
    print(f"Average Amount: ${stats.average_amount:,.2f}")