🚀 Crypto Data Pipelines: ETL vs. ELT Dashboard
Project Overview
This project is a real-time data engineering demonstration. It compares two distinct architectural patterns—ETL and ELT—using a Live Crypto API. It consists of a central Landing Page that connects to two separate Flask microservices.
📂 How the Architecture Works
1. Website A: ETL (Extract, Transform, Load)
The Process: Data is fetched → Python cleans the data → Data is saved to the Database.
Logic: All rounding of prices and market share calculations are done within the Python script before the database ever sees it.
Purpose: Ensures only high-quality, "clean" data is stored.
2. Website B: ELT (Extract, Load, Transform)
The Process: Data is fetched → Raw data is saved to "Staging" → SQL transforms the data.
Logic: "Messy" raw strings from the API are dumped into a staging table. A SQL Query (INSERT INTO ... SELECT) is then used to clean and move data into a production table.
Purpose: Ideal for "Big Data" where you want to keep the original raw records for future use.
🛠️ Features
Real-Time API: Fetches live prices for Bitcoin, Ethereum, and more via CoinGecko.
Batch Lineage: Every data pull is assigned a Batch ID to track exactly when and how the data was processed.
Professional UI: Modern dark-mode dashboards with Chart.js (for visuals) and DataTables (for searching).
Smart Fallback: If the network blocks the API, the system automatically uses Simulated Data to remain functional.
🚀 Quick Start Guide
Step 1: Install Dependencies
bash
pip install flask requests
Use code with caution.

Step 2: Run the Dashboards
Open three terminals in Cursor and run:
Landing Page: python main_app.py (Port 5000)
ETL Site: cd website_a_etl && python app.py --port=5001
ELT Site: cd website_b_elt && python app.py --port=5002
Step 3: Access the Project
Open your browser to: http://127.0.0.1:5000
👨‍🏫 Instructor Note
To verify the logic difference, please check:
ETL: The cleaning happens in website_a_etl/app.py (Python functions).
ELT: The cleaning happens in website_b_elt/app.py (SQL ROUND and CAST statements).
