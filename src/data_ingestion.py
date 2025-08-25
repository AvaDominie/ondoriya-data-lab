import datetime
from io import BytesIO
import os
import logging
from time import timezone
import pandas as pd
import io
import requests
import minio 
from snowflake.connector.pandas_tools import write_pandas
from minio.error import S3Error
import snowflake.connector
from dotenv import load_dotenv
from minio import Minio



load_dotenv()



# Logging setup
VSCODE_WORKSPACE_FOLDER = os.getenv('VSCODE_WORKSPACE_FOLDER', os.getcwd())
LOG_DIR = os.path.join(VSCODE_WORKSPACE_FOLDER, 'src/logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'data_ingestion.log')

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(LOG_FILE),
		logging.StreamHandler()
	]
)
logger = logging.getLogger("data_staging")
logger.info("Logger initialized successfully")





# Minio connection
MINIO_CLIENT = Minio(
    os.getenv("MINIO_URL"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)


BASE_URL = os.getenv("ONDORIYA_BASE_URL")

# List of files to process
FILES_TO_INGEST = [
    "faction_distribution.csv",
    "households.csv",
    "language_building_blocks.csv",
    "language_roots.csv",
    "moons.csv",
    "people.csv",
    "planets.csv",
    "region_biome.csv",
    "regions.csv"
]




def main():
    """
    Connects to MinIO, downloads files from a public URL,
    and uploads them to a MinIO bucket.
    """

    for file_name in FILES_TO_INGEST:
        file_url = f"{BASE_URL}/{file_name}"
        logger.info(f"Processing file: {file_name}")

        try:
            # Download the file
            response = requests.get(file_url)
            response.raise_for_status()

            # Upload to MinIO
            MINIO_CLIENT.put_object(
                os.getenv("MINIO_BUCKET_NAME"),
                file_name,
                BytesIO(response.content),
                length=len(response.content)
            )
            logger.info(f"Uploaded {file_name} to MinIO")

        except requests.RequestException as e:
            logger.error(f"Error downloading {file_name}: {e}")
        except S3Error as e:
            logger.error(f"Error uploading {file_name} to MinIO: {e}")


if __name__ == "__main__":
    main()