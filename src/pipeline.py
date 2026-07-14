import os
import json
import logging
import requests
import time
import boto3
from dotenv import load_dotenv

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logging.info("Pipeline started Successfully")

def load_env_configurations():
    """Extract environment variables safely from memory storage with container directory mapping."""
    load_dotenv()
    
    return {
        "url": os.getenv("API_TARGET_URL"),
        "max_pages": int(os.getenv("MAX_PAGE_TO_FETCH", 4)),
        "page_size": int(os.getenv("RECORDS_PER_PAGE", 5)),
        "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_bucket": os.getenv("AWS_BUCKET_NAME"),
        "aws_region": os.getenv("AWS_REGION", "us-east-1")
    }

def clean_and_normalize_posts(post):
    """Normalize and map raw API transaction items into flat analytical reporting rows."""
    return {
        "transaction_id": post.get("id"),
        "associated_user_id": post.get("userId"),
        "content_title": post.get("title", "").strip().upper(),
        "extracted_time": int(time.time())
    }

def execute_paginated_pipeline():
    """Loops through API pages, normalizes structural items, and saves data persistently to AWS S3."""
    logging.info("Initiating Production Pagination Ingestion Engine...")
    configs = load_env_configurations()

    all_extracted_records = []
    current_page = 1

    while current_page <= configs["max_pages"]:
        logging.info(f"Extracting data from page: {current_page} | Records per page limit: {configs['page_size']}")

        query_params = {
            "_page": current_page,
            "_limit": configs["page_size"]
        }

        try:
            response = requests.get(configs["url"], params=query_params, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
        
            if not raw_data or len(raw_data) == 0:
                logging.info("Encountered empty array payload. Terminating page loop process execution loop cleanly.")
                break
                
            logging.info(f"Successfully extracted {len(raw_data)} elements from page {current_page}.")

            for item in raw_data:
                required_keys = ['id', 'userId', 'title'] 

                if not all(key in item for key in required_keys):
                    logging.warning(f"Skipping record {item.get('id', 'unknown')}: Missing mandatory fields.")
                    continue

                cleaned_row = clean_and_normalize_posts(item)
                all_extracted_records.append(cleaned_row)
        
            current_page += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as connection_error:
            logging.error(f"Network connectivity dropout error encountered: {connection_error}")
            break
        except requests.exceptions.HTTPError as http_error:
            logging.error(f"HTTP Status Validation Failure: {http_error}")
            break
        
    logging.info(f"Pagination processing completed. Total rows aggregated: {len(all_extracted_records)}")

    try:
        logging.info("Initializing connection network handshake with AWS S3...")
        
        # Instantiate the verified S3 Client using your secure environment keys
        s3_client = boto3.client(
            's3',
            aws_access_key_id=configs["aws_access_key"],
            aws_secret_access_key=configs["aws_secret_key"],
            region_name=configs["aws_region"]
        )

        s3_partition_path = "raw/year=2026/month=07/day=14/extracted_transactions.json"
        
        json_string_payload = json.dumps(all_extracted_records, indent=2)

        logging.info(f"Uploading metrics directly to S3 bucket: {configs['aws_bucket']}...")
        
        s3_client.put_object(
            Bucket=configs["aws_bucket"],
            Key=s3_partition_path,
            Body=json_string_payload,
            ContentType="application/json"
        )
        
        logging.info(f"🚀 SUCCESS: Data engine states uploaded safely to: s3://{configs['aws_bucket']}/{s3_partition_path}")

    except Exception as aws_error:
        logging.critical(f"❌ PIPELINE FAILURE: Could not upload data to AWS S3. Details: {aws_error}")

if __name__ == "__main__":
    execute_paginated_pipeline()