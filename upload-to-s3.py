### Get a filename from the user and upload it to Google Cloud Storage using boto3
### Ensure endpoints are set up correctly for Google Cloud Storage
import os
import sys
import boto3
import botocore

bucket_name = 'budget-tables-files'

def upload_file_to_s3(file_name, bucket_name):
    s3_client = boto3.client(
        "s3",
        region_name="eu",
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url='https://storage.googleapis.com',
        config=botocore.client.Config(signature_version='s3v4',
                                      request_checksum_calculation='when_required',
                                      response_checksum_validation='when_required')
    )
    
    try:
        object_name = os.path.basename(file_name)
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f"File {file_name} uploaded to bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file: {e}")

if __name__ == "__main__":  
    if len(sys.argv) < 2:
        print("Usage: python upload-to-s3.py <file_name>")
        sys.exit(1)

    file_name = sys.argv[1]

    if not os.path.isfile(file_name):
        print(f"File {file_name} does not exist.")
        sys.exit(1)

    upload_file_to_s3(file_name, bucket_name)
