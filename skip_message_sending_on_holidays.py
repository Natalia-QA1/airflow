import datetime
import logging
import random
from abc import ABC, abstractmethod

import pymsteams
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from requests.exceptions import ConnectionError, HTTPError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

TEAMS_WEBHOOK_URL = "your_webhook"  # TODO: store credentials in a secure way


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
            logging.error("Failed to retrieve a quote")
            return None
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


class TeamsMessageSender:
    MESSAGE_COLOR = "#F8C471"  # TODO: add to variables

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, title, quote_text, quote_author, image_url):
        """Method combines the whole message and send it to MSTeams channel."""
        myTeamsMessage = pymsteams.connectorcard(self.webhook_url)
        myTeamsMessage.color(self.MESSAGE_COLOR)
        myTeamsMessage.title(title)
        myTeamsMessage.text(
            f"**Quote:** {quote_text}\n\n"
            f"**Author:** {quote_author}\n\n"
            f"![Image]({image_url})"
        )

        try:
            myTeamsMessage.send()
            logging.info("Message successfully sent.")
            return
        except (ConnectionError, HTTPError, TimeoutError) as e:
            logging.error(
                f"Failed to send message due to network error: {e}",
                exc_info=True
            )
        except pymsteams.TeamsWebhookException as e:
            logging.error(
                f"Failed to send message due to Teams webhook error: {e}",
                exc_info=True
            )
        except ValueError as e:
            logging.error(
                f"Failed to send message due to value error: {e}",
                exc_info=True
            )


def load_quote(ti):
    try:
        quote_generator = QuoteGeneratorFromQuotableAPI()
        quote_content = quote_generator.get_content()
        if not quote_content:
            logging.error("Failed to generate quote")
            raise ValueError("Failed to generate quote")
        quote_text, quote_author = quote_content
        ti.xcom_push(
            key='quote_text',
            value=quote_text
        )
        ti.xcom_push(
            key='quote_author',
            value=quote_author
        )
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Error while loading quote: {e}")
        raise


def load_image(ti):
    try:
        image_generator = ImageGeneratorFromPiscumAPI()
        image_url = image_generator.get_content()
        if not image_url:
            logging.error("Failed to generate image.")
            raise ValueError("Failed to generate image.")
        ti.xcom_push(
            key='image_url',
            value=image_url
        )
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Error while loading image: {e}")
        raise


def send_message(ti):
    try:
        quote_text = ti.xcom_pull(
            key='quote_text',
            task_ids='load_quote'
        )
        quote_author = ti.xcom_pull(
            key='quote_author',
            task_ids='load_quote'
        )
        image_url = ti.xcom_pull(
            key='image_url',
            task_ids='load_image'
        )
        if not quote_text or not quote_author or not image_url:
            logging.error("Failed to send message due to missing content")
            raise ValueError("Missing content for sending message.")

        message_sender = TeamsMessageSender(TEAMS_WEBHOOK_URL)
        message_sender.send_message(
            "Sent by Natalia Ananeva",
            quote_text,
            quote_author,
            image_url
        )

    except (requests.RequestException, ValueError) as e:
        logging.error(f"Error while sending message: {e}")
        raise


DATES_TO_SKIP = [
    datetime.datetime(2024, 6, 11),
    datetime.datetime(2024, 6, 13),
    datetime.datetime(2024, 6, 15),
    datetime.datetime(2024, 6, 25),
]


def check_date(execution_date):
    """
    Function checks whether current execution date is in the lists of
    dates which should be skipped.
    :param execution_date: current date
    :return: True if not; otherwise False
    """
    return execution_date not in DATES_TO_SKIP


with DAG(
    dag_id='send_massage_msteams_skip_holidays',
    start_date=datetime.datetime(
        year=2024,
        month=6,
        day=1
    ),
    schedule_interval=timedelta(days=1),
    tags=['send_message_teams'],
    description="A DAG to send message to MS Teams channel using webhook. \
                 Message consists of quote and picture. \
                 In this version some holidays should be skipped.",
    catchup=False
) as dag:

    start_op = EmptyOperator(
        task_id='start'
    )

    def skip_if_excluded(**kwargs):
        execution_date = kwargs['execution_date']
        if not check_date(execution_date):
            raise ValueError("Date is excluded, skipping DAG run")

    check_date_op = PythonOperator(
        task_id='check_date',
        python_callable=skip_if_excluded,
        provide_context=True,  # is used to pass Airflow's context variables to the Python callable
    )

    load_quote_op = PythonOperator(
        task_id='load_quote',
        python_callable=load_quote
    )

    load_image_op = PythonOperator(
        task_id='load_image',
        python_callable=load_image
    )

    send_message = PythonOperator(
        task_id='send_message',
        python_callable=send_message
    )

    finish_op = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start_op >> check_date_op
    check_date_op >> finish_op
    check_date_op >> [load_quote_op, load_image_op] >> send_message >> finish_op
