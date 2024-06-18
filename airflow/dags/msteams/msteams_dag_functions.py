from datetime import datetime

import requests
from msteams_project_code import (
    QuoteGeneratorFromQuotableAPI,
    ImageGeneratorFromPiscumAPI,
    RDSLoader,
    S3Loader,
    TeamsMessageSender
)

# TODO: store credentials in secure way
TEAMS_WEBHOOK_URL = "your webhook"

rds_loader = RDSLoader()
s3_loader = S3Loader()
message_sender = TeamsMessageSender(
    TEAMS_WEBHOOK_URL
)
image_generator = ImageGeneratorFromPiscumAPI()
quote_generator = QuoteGeneratorFromQuotableAPI()


def download_quote(ti):

    quote_content = quote_generator.get_content()
    quote_text, quote_author = quote_content

    if rds_loader.check_quote_duplicates(quote_text, quote_author):
        return download_quote(ti)

    ti.xcom_push(
        key="quote_text",
        value=quote_text
    )
    ti.xcom_push(
        key="quote_author",
        value=quote_author
    )


def download_image(ti):

    image_url = image_generator.get_content()

    ti.xcom_push(
        key="image_url",
        value=image_url
    )


def load_into_aws_s3(params, ti, logical_date):
    bucket_name = params["bucket_name"]
    image_url = ti.xcom_pull(
        task_ids="download_image",
        key="image_url"
    )
    s3_image_url = s3_loader.image_upload(
        bucket_name,
        image_url,
        logical_date
    )

    ti.xcom_push(
        key="s3_image_url",
        value=s3_image_url
    )


def load_into_aws_rds(ti):
    s3_image_url = ti.xcom_pull(
        task_ids="load_img_s3",
        key="s3_image_url"
    )
    quote_text = ti.xcom_pull(
        task_ids="download_quote",
        key="quote_text"
    )
    quote_author = ti.xcom_pull(
        task_ids="download_quote",
        key="quote_author"
    )
    image_url = ti.xcom_pull(
        task_ids="download_image",
        key="image_url")
    s3_key = s3_loader.extract_image_name(
        s3_image_url
    )
    rds_loader.load_quote(
        quote_text,
        quote_author,
        image_url,
        s3_key
    )


def send_msg(params, ti):
    quote_text = ti.xcom_pull(
        task_ids="download_quote",
        key="quote_text"
    )
    quote_author = ti.xcom_pull(
        task_ids="download_quote",
        key="quote_author"
    )
    image_url = ti.xcom_pull(
        task_ids="download_image",
        key="image_url"
    )

    message_color = params["message_color"]

    message_sender.send_message(
        "Sent by Natalia Ananeva",
        quote_text,
        quote_author,
        image_url,
        message_color
    )


DATES_TO_SKIP = [
    datetime(2024, 6, 11),
    datetime(2024, 6, 13),
    datetime(2024, 6, 15),
    datetime(2024, 6, 25),
]


def check_date(**kwargs):
    """
    Function checks whether current execution date is in the lists of
    dates which should be skipped.
    """
    execution_date = kwargs["logical_date"].date()
    if execution_date in [date.date() for date in DATES_TO_SKIP]:
        return "finish"
    return [
        "download_quote",
        "download_image"
    ]
