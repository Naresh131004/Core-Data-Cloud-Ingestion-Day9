import os
import json
import logging
import requests
import time
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s-%(levelname)s-%(message)s"
)

def load_env_configurations():
    """Extraction environment variable safely from Memory Storage"""
    load_dotenv()
    return {
        "url":os.getenv("API_TARGET_URL"),
        "max_pages":int(os.getenv("MAX_PAGE_TO_FETCH",3)),
        "page_size":int(os.getenv("RECORDS_PER_PAGE",10)),
        "output_file":os.getenv("DATA_OUTPUT_PATH","output.json")
    }

def clean_and_normalize_posts(post):
    """Normalize and map transaction items into flat analytical rows"""
    return{
        "transaction_id":post.get("id"),
        "associated_user_id":post.get("userId"),
        "content_title":post.get("title","").strip().upper(),
        "extracted_time":int(time.time())
    }

def execute_paginated_pipeline():
    """Loops through API pages, and flattens payload"""
    logging.info("Initializing Pagination Extraction Using Loop...")
    configs=load_env_configurations()

    all_extracted_records=[]
    current_page=1

    while current_page <= configs["max_pages"]:
        logging.info(f"Extracting Data from page: {current_page}, size limit: {configs['page_size']}")

        query_params={
            "_page":current_page,
            "_limit":configs["page_size"]
        }

        try:
            response=requests.get(configs["url"],params=query_params,timeout=10)
            response.raise_for_status()
            raw_data=response.json()
        
            if not raw_data or len(raw_data)==0:
                logging.info("Encountering empty array. Terminating page loop")
                break
            logging.info(f"Successfully Extracted {len(raw_data)} raw elemenst from current page {current_page}")

            for item in raw_data:
                cleaned_row=clean_and_normalize_posts(item)
                all_extracted_records.append(cleaned_row)
        
            current_page+=1

            time.sleep(0.5)

        except requests.exceptions.RequestException as connection_error:
            logging.error(f"Error: {connection_error}")
            break

        except requests.exceptions.HTTPError as http_error:
            logging.error(f"HTTP STATUS FAILURE: {http_error}")
            break
        
    logging.info(f"Pagination Completed! Totally there are {len(all_extracted_records)} rows.")

    with open(configs["output_file"],"w") as target_file:
        json.dump(all_extracted_records, target_file, indent=2)

    logging.info(f"All the extractred data are stored in the {configs['output_file']}")

if __name__ == "__main__":
    execute_paginated_pipeline()