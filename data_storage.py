import os

# Buat direktori data lake
os.makedirs("data_lake/csv", exist_ok=True)
os.makedirs("data_lake/pdf", exist_ok=True)
os.makedirs("data_lake/txt", exist_ok=True)

print("Data Lake structure created!")
