import logging

import pandas as pd
from pandas import read_excel
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# setup logging config
logging.basicConfig(
    filename = "C:\\Data_Stewardship\\datastewardship_error.log",
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s -%(message)s'
)

# config for snowflake
snowflake_user = 'email'
snowflake_password = 'password'
snowflake_account = 'your_work_environment' #EXAMPLE: PROD,DEV
snowflake_warehouse = 'warehouse' #EXAMPLE: SMALL_S01
snowflake_database = 'database' #EXAMPLE: COMM_PROD_DB
snowflake_schema = 'schema' #EXAMPLE: SANDBOX
snowflake_role = 'role' #EXAMPLE: COMM_ETL_RW


print("----------------------------------------------------------")
print("---------------DATA_STEWARDSHIP_SCRIPT--------------------")
print("----------------------------------------------------------")

#load the data from source in xlsx
input_file = "C:\\Data_Stewardship\\data.xlsx"
#custom sql query
sql_file = "C:\\Data_Stewardship\\query.sql"

#establish connection in snowflake

conn = snowflake.connector.connect(
    user = snowflake_user,
    password = snowflake_password,
    account = snowflake_account,
    warehouse = snowflake_warehouse,
    database = snowflake_database,
    schema = snowflake_schema,
    role = snowflake_role
)
cursor = conn.cursor()
if conn is None  or conn.is_closed():
    print("Reconnecting to snowflake...")
    conn = snowflake.connector.connect(...)
else:
    print("Connection Successful.")

# upload file to temp_table
def upload_to_snowflake(conn, df, temp_upload):
    try:
        cursor.execute("CREATE OR REPLACE TEMP TABLE TEMP_UPLOAD (FIRST_NAME STRING, LAST_NAME STRING, ADDRESS_LINE1 STRING, STATE STRING)")
        write_pandas(conn, df, 'temp_upload')
        print(f"Data uploaded to Snowflake table: temp_upload")
    except Exception as e:
        print(f"Error Uploading table to snowflake.")
def execute_custom_query(conn, query):
    try:
        df_result = pd.read_sql(query,conn)
        return df_result
    except Exception as e:
        print(f"Error executing query: {e}")
def read_sql_query(sql_file):
    try:
        with open(sql_file,"r") as file:
            return file.read().strip()
    except FileNotFoundError:
        print(f"File {sql_file} not found. Exiting Program")
        exit()

if __name__ == "__main__":
    df_input = read_excel(input_file)
    temp_table = "temp_upload"
    upload_to_snowflake(conn, df_input, temp_table)
    choice = input("Enter '1' to use default Matching query, '2' to use your custom query: ")
    if choice == 1:
        match_query = f"""
        With
        IDS AS 
        (
        SELECT First name AS Input_FNAME, Last_name AS Input_LNAME, Address line_1 AS Input_ADDRESS_LINE1, State AS Input_STATE
        FROM "COMM_DMART_PROD_DB","SANDBOX". "temp_upload" 
        ),
        OK AS
        (
        SELECT OK_.ENTITY URI AS OK_URI, FIRST_NAME AS OK_FNAME, LAST_NAME AS OK_LNAME, TITLE_LKP AS OK_TITLE,
        ADDRESS_LINE1 AS OK_ADDRESS, STATE_PROVINCE_LKP AS OK_STATE, 
        FROM "COMM_DMART_PROD". "CUSTOMER SL". "HCP" OK_
        JOIN "COMM_DMART_PROD". "CUSTOMER SL". "ADDRESSES" OK_ADD      
        ON OK_HCP.ENTITY_URI = OK_ADD.ENTITY_URI 
        
        WHERE 
        --change these conditions according to your table data
        
        OK_.SOURCE_MATCH_CATEGORY != 99
        AND OK_.STATUS_LKP = 'HCPStatus:A' 
        
        )
        SELECT DISTINCT *,
        CASE
            WHEN Input_FNAME IS NOT NULL AND Input_FNAME != ''
            THEN JAROWINKLER_SIMILARITY(LOWER(Input_FNAME), LOWER(OK_FNAME))
            ELSE NULL
        END AS FName_Score,
        CASE
            WHEN Input_LNAME IS NOT NULL AND Input_LNAME != ''
            THEN JAROWINKLER_SIMILARITY(LOWER(Input_LNAME), LOWER(OK_LNAME))
            ELSE NULL
        END AS LName_Score,
        CASE
            WHEN Input_ADDRESS_LINE1 IS NOT NULL AND Input_ADDRESS_LINE1 != ''
            THEN JAROWINKLER_SIMILARITY(LOWER(Input_ADDRESS_LINE1), LOWER (OK_ADDRESS))
            ELSE NULL
        END AS Address_Score,
        CASE 
            WHEN Input_STATE IS NOT NULL AND Input_STATE != ''
            THEN JAROWINKLER_SIMILARITY (LOWER(Input_STATE),LOWER(OK_STATE))
            ELSE NULL
        END AS STATE_SCORE
        FROM IDS
        LEFT JOIN OK 
        WHERE
            ((Input_FNAME IS NOT NULL AND JAROWINKLER_SIMILARITY(LOWER(Input_FNAME), LOWER(OK_FNAME)) >= 85
            OR JAROWINKLER_SIMILARITY(LOWER(Input_FNAME), LOWER (OK_LNAME)) >= 85)
            AND (Input_LNAME IS NOT NULL AND JAROWINKLER_SIMILARITY(LOWER(Input_LNAME), LOWER (OK_LNAME)) >= 85
            OR JAROWINKLER_SIMILARITY(LOWER (Input_LNAME), LOWER (OK_FNAME)) >= 85)
            AND (Input_ADDRESS_LINE1 IS NULL OR JAROWINKLER_SIMILARITY(LOWER(Input_ADDRESS_LINE1), LOWER(OK_ADDRESS)) > 85)
            OR (Input_STATE IS NULL AND JAROWINKLER_SIMILARITY(LOWER(Input_STATE), LOWER (OK_STATE)) >= 85))
        ORDER BY 1, JAROWINKLER_SIMILARITY(LOWER(Input_ADDRESS_LINE1), LOWER(OK_ADDRESS)) DESC;
        """
    elif choice == "2":
        match_query = read_sql_query(sql_file)
    else:
        print("Invalid choice! Exiting Program.")
        exit()
#Output
    df_result = execute_custom_query(conn, match_query)
    output_file = "C:\\Data_Stewardship\\output.xlsx"
    df_result.to_excel(output_file, index=False)
    print(f"Data consolidation complete. Output written to {output_file}")
    try:
        cursor.execute("DROP TABLE IF EXISTS temp_upload")
        print("Deletion successful for temp_table.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error deleting table.")
print("----------------------------------------------------------")
print("----------------------------------------------------------")
