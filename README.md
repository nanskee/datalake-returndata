<img width="1920" alt="Slide 16_9 - 32" src="https://github.com/user-attachments/assets/66d16449-2f8b-45ba-904b-20de3fa3f62f" />

**Data Pipeline Architecture for Analytics**

This architecture outlines the process of collecting, transforming, and analyzing product return data to generate actionable insights. Below is a step-by-step breakdown of the data pipeline:

- **Data Sources**:  
  - Data is collected from various formats:  
    - PDF files (.pdf)  
    - Text files (.txt)  
    - CSV files (.csv)  
  - These files contain product return data from multiple sources.

- **Data Lake**:  
  - Raw data from the sources is loaded into a **data lake** using Python.  
  - This serves as the initial storage for unprocessed data.

- **Staging Database**:  
  - Data is transferred to a **staging database** via SQL Server Management Studio (SSMS).  
  - The staging database contains:  
    - Operational data from the **OLTP AdventureWorks2019** database.  
    - Return data from the data lake.

- **ETL Process**:  
  - The **ETL (Extract, Transform, Load)** process is performed to extract data from the staging database, transform it into a suitable format, and load it into a **PostgreSQL-based data warehouse**.

- **Data Warehouse**:  
  - The data warehouse acts as a **centralized repository** for structured and processed data.

- **Analytics and Reporting**:  
  - Processed data is imported into **Power BI** for analytics and reporting.  
  - Visualizations are created to analyze the number of orders and returns over the months in 2011.
