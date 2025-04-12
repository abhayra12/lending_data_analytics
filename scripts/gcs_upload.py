#!/usr/bin/env python3
"""
Combined script that handles:
1. Downloading Kaggle data directly to the data folder and uploading to GCS
2. Uploading Python scripts and credentials to GCS

This script handles the Kaggle dataset download properly by:
1. Using kagglehub to download the dataset files 
2. Finding CSV files in the downloaded directory
3. Copying the target CSV file to the data directory
4. Uploading the CSV file to the GCS bucket
"""
import os
import sys
import zipfile
import shutil
import argparse
import kagglehub
from google.cloud import storage

def ensure_dir(directory):
    """Ensure directory exists, create if necessary."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_kaggle_data(data_dir="data"):
    """Download dataset from Kaggle to specified directory."""
    print(f"Downloading dataset from Kaggle to {data_dir}...")
    ensure_dir(data_dir)
    
    # Download using kagglehub - this returns a directory path, not a zip file
    kaggle_dir = kagglehub.dataset_download("wordsforthewise/lending-club")
    print(f"Dataset downloaded to directory: {kaggle_dir}")
    
    # Look for the CSV file directly in the downloaded directory
    csv_files = []
    for root, _, files in os.walk(kaggle_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in downloaded Kaggle dataset: {kaggle_dir}")
    
    # Find the accepted_2007_to_2018Q4.csv file
    target_csv = None
    for csv_file in csv_files:
        if "accepted_2007_to_2018Q4.csv" in csv_file:
            target_csv = csv_file
            break
    
    if not target_csv:
        # If we can't find the specific file, use the first CSV
        target_csv = csv_files[0]
        print(f"Specific CSV file not found, using: {os.path.basename(target_csv)}")
    else:
        print(f"Found target CSV file: {os.path.basename(target_csv)}")
    
    # Copy to data directory
    dest_path = os.path.join(data_dir, os.path.basename(target_csv))
    shutil.copy2(target_csv, dest_path)
    print(f"CSV file copied to: {dest_path}")
    
    return dest_path

def extract_data(zip_path, extract_dir="data"):
    """Extract the downloaded zip file."""
    print(f"Extracting data to {extract_dir}...")
    ensure_dir(extract_dir)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print("Extraction complete.")
    return extract_dir

def upload_file_to_gcs(local_file_path, bucket_name, destination_blob_name, storage_client=None):
    """Upload a single file to Google Cloud Storage."""
    print(f"Uploading {local_file_path} to gs://{bucket_name}/{destination_blob_name}")
    
    if storage_client is None:
        storage_client = storage.Client()
        
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")

def cleanup(zip_path, extract_dir=None):
    """Clean up temporary files."""
    if os.path.exists(zip_path):
        print(f"Deleting zip file: {zip_path}")
        os.remove(zip_path)
    
    if extract_dir and os.path.exists(extract_dir) and extract_dir != "data":
        print(f"Removing extraction directory: {extract_dir}")
        shutil.rmtree(extract_dir)

def upload_kaggle_data(bucket_name, client=None):
    """Download Kaggle data and upload to GCS"""
    # Download dataset directly (returns path to CSV file)
    csv_file_path = download_kaggle_data()
    
    # Get the filename for the destination blob
    csv_filename = os.path.basename(csv_file_path)
    
    # Upload to GCS
    if os.path.exists(csv_file_path):
        upload_file_to_gcs(
            csv_file_path,
            bucket_name,
            f"data/{csv_filename}",
            client
        )
    else:
        print(f"Error: File not found: {csv_file_path}")
        return False
    
    print("Kaggle data upload completed successfully.")
    return True

def upload_scripts(bucket_name, creds_path="gcp-creds.json", script_dir="scripts", client=None):
    """Upload Python scripts and credentials to GCS bucket"""
    # Initialize GCS client with credentials if not provided
    if client is None:
        if os.path.exists(creds_path):
            client = storage.Client.from_service_account_json(creds_path)
        else:
            client = storage.Client()
    
    # List of scripts to upload
    scripts = [
        "01-lending_club_intro.py",
        "02-data_cleaning_customers.py",
        "03-data_cleaning_loans.py",
        "04-data_cleaning_repayment.py",
        "05-data_cleaning_defaulters.py",
        "06-data_cleaning_defaulters_detailed.py",
        "07-create_bq_ext_tables.py",
        "08-create_unified_view.py",
        "09-filter-bad-data.py",
        "10-loan_score.py"
    ]
    
    # Upload scripts
    for script in scripts:
        script_path = os.path.join(script_dir, script)
        if os.path.exists(script_path):
            upload_file_to_gcs(script_path, bucket_name, f"code/{script}", client)
        else:
            print(f"Warning: {script_path} not found")
    
    # Upload credentials
    if os.path.exists(creds_path):
        upload_file_to_gcs(creds_path, bucket_name, "code/gcp-creds.json", client)
    else:
        print(f"Error: Credentials file {creds_path} not found")
        return False
    
    print("Scripts upload completed successfully!")
    return True

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Upload data and scripts to GCS")
    parser.add_argument("bucket_name", help="Name of GCS bucket")
    parser.add_argument("--creds", default="gcp-creds.json", help="Path to GCP credentials JSON file")
    parser.add_argument("--script-dir", default="scripts", help="Directory containing Python scripts to upload")
    parser.add_argument("--kaggle", action="store_true", help="Download and upload Kaggle data")
    parser.add_argument("--scripts", action="store_true", help="Upload Python scripts")
    parser.add_argument("--all", action="store_true", help="Upload both Kaggle data and scripts")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not (args.kaggle or args.scripts or args.all):
        parser.print_help()
        print("\nError: Please specify at least one action (--kaggle, --scripts, or --all)")
        return 1
    
    # Initialize GCS client
    try:
        if os.path.exists(args.creds):
            client = storage.Client.from_service_account_json(args.creds)
            print(f"Using credentials from {args.creds}")
        else:
            client = storage.Client()
            print("Using default credentials")
    except Exception as e:
        print(f"Error initializing GCS client: {e}")
        return 1
    
    success = True
    
    # Upload Kaggle data if requested
    if args.kaggle or args.all:
        try:
            success = upload_kaggle_data(args.bucket_name, client) and success
        except Exception as e:
            print(f"Error uploading Kaggle data: {e}")
            success = False
    
    # Upload scripts if requested
    if args.scripts or args.all:
        try:
            success = upload_scripts(args.bucket_name, args.creds, args.script_dir, client) and success
        except Exception as e:
            print(f"Error uploading scripts: {e}")
            success = False
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

'''
Execution Guide:
---------------
1. Ensure you have the required packages installed:
   pip install -r requirements.txt

2. Authenticate with Google Cloud:
   - Either set the GOOGLE_APPLICATION_CREDENTIALS environment variable:
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   - Or authenticate using gcloud:
     gcloud auth application-default login
   - Or specify a credentials file with --creds

3. Authenticate with Kaggle:
   - Either set up your Kaggle API credentials in ~/.kaggle/kaggle.json
   - Or set the KAGGLE_USERNAME and KAGGLE_KEY environment variables

4. Run the script with one of these options:
   - Download and upload Kaggle data: python scripts/gcs_upload.py your-bucket-name --kaggle
   - Upload Python scripts: python scripts/gcs_upload.py your-bucket-name --scripts
   - Do both: python scripts/gcs_upload.py your-bucket-name --all
   
   Additional options:
   - Specify a custom scripts directory: --script-dir=/path/to/scripts
   - Specify a custom credentials file: --creds=/path/to/credentials.json
''' 