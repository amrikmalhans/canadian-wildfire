import requests
import os
import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


activefires_url = "https://cwfis.cfs.nrcan.gc.ca/downloads/activefires/"
activefires_csv = "activefires.csv"


def fetch_csv():
    try:
        csv_index_response = requests.get(activefires_url)
        csv_index_response.raise_for_status()

        soup = BeautifulSoup(csv_index_response.text, "html.parser")

        table = soup.find("table")
        table_rows = table.find_all("tr")

        last_modified = None

        for row in table_rows:
            td_file = row.find_all("td", class_="file")
            for td in td_file:
                if activefires_csv in td.text:
                    td_last_modified = row.find_all("td", class_="date")
                    last_modified = td_last_modified[0].text.replace(" ", "-")

                    break

        if not last_modified:
            logger.error("Failed to find last modified date")
            raise Exception("Failed to find last modified date")

        csv_file_response = requests.get(activefires_url + activefires_csv)
        csv_file_response.raise_for_status()

        csv_content = csv_file_response.content.decode("utf-8")

        print(last_modified)

        return csv_content, last_modified

    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch CSV file: %s", e)
        raise Exception("Failed to fetch CSV file") from e
    except Exception as e:
        logger.error("Failed to fetch CSV file: %s", e)
        raise


def save_to_s3(bucket_name, s3_key, csv_content):
    try:

        aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION")

        s3_client = boto3.client(
            "s3",
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_content)

    except (ClientError, BotoCoreError) as e:
        logger.error("Failed to save CSV file to S3: %s", e)
        raise Exception("Failed to save CSV file to S3") from e
    except Exception as e:
        logger.error("An error occurred: %s", e)
        raise


if __name__ == "__main__":
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not bucket_name:
        logger.error("S3_BUCKET_NAME environment variable is not set")
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    try:
        csv_content, last_modified = fetch_csv()
        s3_key = f"activefires/activefires-{last_modified}.csv"
        save_to_s3(bucket_name, s3_key, csv_content)
    except Exception as e:
        logger.error("An error occurred: %s", e)
        raise
