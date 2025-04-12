# 🧹 Snowflake Data Stewardship Automation

This Python-based data stewardship utility automates the process of loading Excel files into Snowflake, executing custom or default matching queries using fuzzy logic, and exporting matched results. 
It’s designed for teams working with healthcare or customer datasets, offering a plug-and-play pipeline for data enrichment, cleansing, or deduplication.

---

## 🚀 Features

- ✅ Upload Excel files to Snowflake
- 📦 Automatically creates temporary staging tables
- 🔍 Performs fuzzy matching (e.g., using JAROWINKLER_SIMILARITY)
- 📝 Supports custom SQL queries
- 📤 Exports matched results to Excel
- 📚 Logging of errors for troubleshooting

---

## 🛠️ Requirements

Install dependencies:

```bash
    pip install pandas
    pip install snowflake-connector-python[pandas]
    pip install openpyxl
```

⚙️ Configuration
Edit these variables in main.py:

    snowflake_user = 'your_email'
    snowflake_password = 'your_password'
    snowflake_account = 'your_account_id'
    snowflake_warehouse = 'your_warehouse'
    snowflake_database = 'your_database'
    snowflake_schema = 'your_schema'
    snowflake_role = 'your_role'

📦 How to Run
Place your input file (data.xlsx) and optional query.sql in the working directory.

  1. Run the script:
  ```
    python data_stewardship.py
  ```
  2. Choose whether to run the default matching logic or use a custom query.
  3. Output is saved to output.xlsx.

🧠 Matching Logic (Default)
    If no custom SQL is provided, the script uses default fuzzy matching logic using:
    
      First Name similarity
      Last Name similarity
      Address Line 1 similarity
      State similarity
      
  It uses JAROWINKLER_SIMILARITY to compute string similarity in SQL.
   
