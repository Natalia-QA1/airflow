from datetime import datetime
import logging
import random
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import boto3
import psycopg2
from psycopg2 import sql
import pymsteams
import requests
from airflow.models import (
    Variable,
    Connection
)
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from requests.exceptions import ConnectionError, HTTPError


class ContentGeneratorBaseException(Exception):
    pass


class ContentGetException(ContentGeneratorBaseException):
    pass


class S3LoaderBaseException(Exception):
    pass


class S3LoaderEmptyValue(S3LoaderBaseException):
    pass


class ContentGenerator(ABC):
    @abstractmethod
    def get_content(self):
        """ Method create connection to API and select random image/quote/etc."""
        pass


class QuoteGeneratorFromQuotableAPI(ContentGenerator):
    QUOTABLE_API_URL = Variable.get("quotable_api_url")

    def get_content(self):
        response = requests.get(self.QUOTABLE_API_URL)

        if response.status_code != 200:
            logging.error(
                f"Failed to retrieve a quote. Status code: {response.status_code}"
            )
            raise ContentGetException(f"Failed to retrieve a quote. Status code: {response.status_code}")
        else:
            data = response.json()
            quote_text = data["content"]
            quote_author = data["author"]
            logging.info("A quote successfully generated.")

            return quote_text, quote_author


class ImageGeneratorFromPiscumAPI(ContentGenerator):
    WIDTH = random.randint(300, 800)
    HEIGHT = random.randint(300, 800)
    PISCUM_API_URL = Variable.get("piscum_api_url")

    def get_content(self):
        image_url = f"{self.PISCUM_API_URL}/{self.WIDTH}/{self.HEIGHT}"
        return image_url


class S3Loader:
    def __init__(self):
        self.s3_key = None
        self.s3_hook = self.get_s3_hook()

    def get_s3_hook(self):
        s3_hook = S3Hook(aws_conn_id="aws_s3")
        return s3_hook

    def extract_image_name(self, url):

        if not url:
            raise S3LoaderEmptyValue("Extracted URL is None or empty")

        parsed_url = urlparse(url)
        img_name = parsed_url.path.split("/")[-1]

        if not img_name:
            raise S3LoaderEmptyValue("Extracted image name is None or empty")
        return img_name

    def image_upload(self, bucket_name, image_url, timestamp):
        try:

            image_data = image_url
            timestamp = timestamp.strftime("%Y%m%d%H%M%S")
            image_name_from_url = self.extract_image_name(image_url)
            image_name = f"{image_name_from_url}_{timestamp}.jpg"
            s3_key = f"{image_name}"

            self.s3_hook.load_bytes(
                image_data,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True,
                encrypt=False,
                acl_policy='private'
            )

            s3_image_url = f"{self.s3_hook.get_conn().meta.endpoint_url}/{bucket_name}/{s3_key}"
            logging.info(f"Image successfully uploaded to S3: {s3_image_url}")
            return s3_image_url

        except boto3.exceptions.Boto3Error as error:
            logging.error(
                f"Failed to upload image to S3: {error}",
                exc_info=True
            )
            raise error


class RDSLoader:

    def __init__(self, conn_id='aws_postgres'):
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = self.hook.get_conn()

    def check_quote_duplicates(self, quote_text, quote_author):
        try:
            with self.conn.cursor() as cursor:
                select_query = """
                    SELECT * FROM daily_quotes
                    WHERE quote = %s AND quote_author = %s
                    LIMIT 1
                """
                cursor.execute(
                    select_query, (
                        quote_text,
                        quote_author
                    )
                )
                record = cursor.fetchone()
                if record:
                    logging.info("Duplicate quote found.")
                    return record
                else:
                    logging.info("No duplicate quote found.")
                    return None

        except psycopg2.Error as error:
            logging.error(
                f"Failed to perform duplicated check: {error}",
                exc_info=True)
            raise error

    def load_quote(self, quote_text, quote_author, image_url, s3_key):
        try:
            with self.conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO daily_quotes (
                        quote, 
                        quote_author,
                        picture_url, 
                        image_aws_s3_key
                    )
                    VALUES (%s, %s, %s, %s)
                """)
                cursor.execute(
                    insert_query,
                    (
                        quote_text,
                        quote_author,
                        image_url,
                        s3_key
                    )
                )
                self.conn.commit()
                logging.info("Quote successfully saved to RDS.")
        except psycopg2.Error as error:
            logging.error(
                f"Failed to save quote to RDS: {error}",
                exc_info=True)
            raise error


class TeamsMessageSender:

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, title, quote_text, quote_author, image_url, message_color):
        """Method combines the whole message and send it to MSTeams channel."""
        myTeamsMessage = pymsteams.connectorcard(self.webhook_url)
        myTeamsMessage.color(message_color)
        myTeamsMessage.title(title)
        myTeamsMessage.text(
            f"**Quote:** {quote_text}\n\n"
            f"**Author:** {quote_author}\n\n"
            f"![Image]({image_url})"
        )

        try:
            myTeamsMessage.send()
            logging.info(
                "Message successfully sent."
            )
            return
        except (
                ConnectionError,
                HTTPError,
                TimeoutError
        ) as error:
            logging.error(
                f"Failed to send message due to network error: {error}",
                exc_info=True
            )
            raise error
        except pymsteams.TeamsWebhookException as error:
            logging.error(
                f"Failed to send message due to Teams webhook error: {error}",
                exc_info=True
            )
            raise error
