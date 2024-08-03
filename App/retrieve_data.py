import io
import os
import tempfile
import cdsapi
import boto3
from dotenv import load_dotenv
@@ -28,27 +28,31 @@

client = cdsapi.Client()

# In-memory buffer
buffer = io.BytesIO()

print("Attempting to retrieve and upload data directly to S3...")

try:
    # Retrieve data and save it to the buffer
    client.retrieve(dataset, request).download(target=None, callback=lambda data: buffer.write(data))
    buffer.seek(0)  # Go back to the start of the buffer

    # Upload the buffer content to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    s3_key = 'App/Data/data.grib'
    s3_client.upload_fileobj(buffer, BUCKET_NAME, s3_key)
    print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")

except Exception as e:
    print(f"Error: {e}")
# Create a temporary file to store the data
with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file_path = temp_file.name
    print(f"Attempting to retrieve data to temporary file: {temp_file_path}")

    try:
        # Retrieve data and save it to the temporary file
        client.retrieve(dataset, request).download(temp_file_path)
        print(f"Data download completed. File saved to: {temp_file_path}")

        # Upload the temporary file to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_key = 'App/Data/data.grib'
        s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
        print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)
