import os
import time
import argparse
from google.cloud import storage
from google.oauth2 import service_account
from airflow.models import Variable
import json


execution_path = Variable.get("execution_path")


def upload_files_to_gcs(bucket_name, folder_path, destination_folder):
    # Initialize the Google Cloud Storage client
    storage_credentials = service_account.Credentials.from_service_account_info(json.loads(Variable.get("google_cloud_secret")))
    storage_client = storage.Client(credentials=storage_credentials)
        
    print(storage_client)
    # Get the bucket object
    bucket = storage_client.get_bucket(bucket_name)

    # List files in the local folder
    files_to_upload = os.listdir(folder_path)

    for file_name in files_to_upload:
        if file_name != 'README.md':
            # File with naming convention
            timestamp = int(time.time())
            new_file_name = f"{os.path.splitext(file_name)[0]}_{timestamp}{os.path.splitext(file_name)[1]}"

            # Specify the full path for both local and GCS files
            local_file_path = os.path.join(folder_path, file_name)
            gcs_file_path = f"{destination_folder}/{new_file_name}" 

            # Upload the file to Google Cloud Storage
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(local_file_path)

            print(f"Uploaded {local_file_path} to GCS as {gcs_file_path}")

            if blob.exists():
                # Remove the local file
                os.remove(local_file_path)
                print(f"Removed {local_file_path} from the local folder")



def main():
    parser = argparse.ArgumentParser(description="Rename files and upload to Google Cloud Storage")
    parser.add_argument("--bucket_name", required=True, help="Google Cloud Storage bucket name")
    parser.add_argument("--folder_path", required=True, help="Path to the folder containing files to rename and upload")
    parser.add_argument("--destination_folder", required=True, help="Destination folder within the bucket")    

    args = parser.parse_args()
    
    upload_files_to_gcs(args.bucket_name, args.folder_path, args.destination_folder)


if __name__ == "__main__":
    main()
