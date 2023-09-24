import psycopg2
import pandas as pd
import os
from sqlalchemy import create_engine
from airflow.models import Variable

# PostgreSQL database connection settings
formatted_zone_settings = {
    "dbname": Variable.get("fz_dbname"),
    "user": Variable.get("dbuser"),
    "password": Variable.get("formatted_zone_secret"),
    "host": Variable.get("dbhost"),
}




def remove_duplicates(df):
    return df.drop_duplicates()


def remove_nulls(df):
    return df.dropna()

def load_to_trusted_zone(engine, table_name, df):
    df.to_sql(table_name, engine, if_exists='replace', index=False) 

def retrieve_table_names(db_settings):
    conn = psycopg2.connect(**db_settings)
    cursor = conn.cursor()

    # Retrieve all table names from the database
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    table_names = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return table_names

def extract_table_name_without_timestamp(table_name):
    # Extract the table name without the timestamp
    return table_name.rsplit("_", 1)[0]

def group_tables_by_name(table_names):
    table_groups = {}
    for table_name in table_names:
        # Extract the table name without the timestamp
        name_without_timestamp = extract_table_name_without_timestamp(table_name)

        if name_without_timestamp not in table_groups:
            table_groups[name_without_timestamp] = []

        table_groups[name_without_timestamp].append(table_name)

    return table_groups

def load_tables_into_dataframes(db_settings, table_names):
    conn = psycopg2.connect(**db_settings)

    dataframes = {}
    for table_name in table_names:
        # Load table into a Pandas DataFrame
        query = f"SELECT * FROM \"{table_name}\""
        df = pd.read_sql_query(query, conn)
        dataframes[table_name] = df

    conn.close()

    return dataframes

def combine_tables_into_dfs(dataframes, table_groups):
    combined_dataframes = {}
    for name_without_timestamp, tables in table_groups.items():
        combined_df = pd.concat([dataframes[table] for table in tables], axis=0, ignore_index=True)
        combined_dataframes[name_without_timestamp] = combined_df

    return combined_dataframes

def main():
    table_names = retrieve_table_names(formatted_zone_settings)
    table_groups = group_tables_by_name(table_names)
    dataframes = load_tables_into_dataframes(formatted_zone_settings, table_names)
    
    combined_dataframes = combine_tables_into_dfs(dataframes, table_groups)
    engine = create_engine(f'postgresql://{Variable.get("dbuser")}:{Variable.get("trusted_zone_secret")}@{Variable.get("dbhost")}:5432/{Variable.get("tz_dbname")}')

    # Iterate each combined dataframe
    for name_without_timestamp, df in combined_dataframes.items():
        print(f"Combined DataFrame for Table Name: {name_without_timestamp}")
        df = remove_duplicates(df)
        df = remove_nulls(df)
        load_to_trusted_zone(engine, name_without_timestamp, df)
        print(df.head()) 

if __name__ == "__main__":
    main()
