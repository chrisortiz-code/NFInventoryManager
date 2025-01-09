import json
import sqlite3
import psycopg2
from psycopg2 import sql, extras

# PostgreSQL DB config
db_config = json.load(open('config.json'))

def upload_dno_to_postgres(sqlite_path="dno.db"):
    try:
        # Connect to SQLite
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()

        # Fetch articles from SQLite dno table
        sqlite_cursor.execute("SELECT article FROM dno")
        rows = sqlite_cursor.fetchall()
        print(f"Number of articles fetched from SQLite: {len(rows)}")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return
    finally:
        if sqlite_conn:
            sqlite_conn.close()

    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**db_config)
        pg_cursor = pg_conn.cursor()
        print("Connected to PostgreSQL successfully.")

        # Create dno table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dno (
            article VARCHAR(25) NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (article)
        );
        """
        pg_cursor.execute(create_table_query)
        pg_conn.commit()
        print("Ensured that the dno table exists in PostgreSQL.")

        # Prepare data for insertion
        # Each row from SQLite is a tuple like (article,)
        # We need to add the 'active' value as True
        data_to_insert = [(article[0], True) for article in rows]

        # Define the INSERT statement with ON CONFLICT to ignore duplicates
        insert_query = """
        INSERT INTO dno (article, active)
        VALUES %s
        ON CONFLICT (article) DO NOTHING;
        """

        # Use execute_values for efficient bulk insertion
        extras.execute_values(
            pg_cursor, insert_query, data_to_insert, template=None, page_size=100
        )
        pg_conn.commit()
        print(f"Inserted {pg_cursor.rowcount} new articles into PostgreSQL dno table.")

    except psycopg2.Error as e:
        print(f"PostgreSQL error: {e}")
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    upload_dno_to_postgres("dno.db")
