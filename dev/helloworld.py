import os
import snowflake.connector

def main():
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.getenv("SF_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SF_ACCOUNT"),
            warehouse=os.getenv("SF_WAREHOUSE"),
            database=os.getenv("SF_DATABASE"),
            role=os.getenv("SF_ROLE")
        )
        print("Connection successful.")
        
        # Split the SQL script into individual statements
        sql_statements = [
            """
            CREATE or replace TABLE GIT_INT.DEMO.TEST_TABLE (
                ID INT,
                NAME STRING
            )
            """,
            """
            INSERT INTO GIT_INT.DEMO.TEST_TABLE VALUES (1, 'Example')
            """
        ]
        
        with conn.cursor() as cur:
            print("Executing SQL script...")
            for sql in sql_statements:
                print(f"Executing: {sql.strip()}")
                cur.execute(sql)
            print("SQL script executed successfully.")
        
        conn.close()
        print("Connection closed.")
    
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
    print("Hello World from the Devops repo!")
