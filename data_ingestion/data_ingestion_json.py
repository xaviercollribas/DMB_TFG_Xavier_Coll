import requests
import os
import pandas as pd
import argparse
import sys
import json
from airflow.models import Variable

# ENVIRONMENT VARIABLES

execution_path = Variable.get("execution_path")

def retrieve_data(endpoint):
    response = requests.get(endpoint)
    correct_request = False
    if response.status_code == 200:
        print("API 1: CORRECT RESPONSE")
        correct_request = True
    else:
        print(f"API 1: ERROR. ERROR CODE: {response.status_code}")

    if correct_request:
        return response
    else:
        return None

def generate_file(FILENAME, TEMPORAL_LANDING_ZONE_PATH):
    file_name = f"{FILENAME}.csv"
    file_path = os.path.join(TEMPORAL_LANDING_ZONE_PATH, file_name)
    return file_path


def main():

    # ARGUMENT PARSER FOR THE CLI
    parser = argparse.ArgumentParser(description="Process command line arguments.")
    parser.add_argument("--api_endpoint", required=True, help="API endpoint URL")
    parser.add_argument("--temporal_landing_zone_path", required=True, help="Temporal landing zone path")
    parser.add_argument("--filename", required=True, help="Filename")

    args = parser.parse_args()

    API_ENDPOINT = args.api_endpoint
    TEMPORAL_LANDING_ZONE_PATH = args.temporal_landing_zone_path
    FILENAME = args.filename

    # RETRIEVE DATA FROM API IN A CSV FORMAT AND LOAD IT IN A DATAFRAME
    print(f"Fetching data from {API_ENDPOINT}")
    response = retrieve_data(API_ENDPOINT)
    print("----------------------")
    print(response.json())
    print(response.json()['elements'])
    df = pd.DataFrame(response.json()['elements'])
    print(df.head)
    if response is not None:
        print("Generating CSV file")
        file_path = generate_file(FILENAME, TEMPORAL_LANDING_ZONE_PATH)
        print(f"Storing file in {file_path}")   
        df.to_csv(file_path, index=False, sep=';')
        print("Data stored in temporal landing zone correctly as CSV")
    else:
        sys.exit(f"There was an error while retrieving data from {API_ENDPOINT}")

if __name__ == '__main__':
    main()
