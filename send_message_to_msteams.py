import logging
import random
from abc import ABC, abstractmethod

import pymsteams
import requests
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
    QUOTABLE_API_URL = "https://api.quotable.io/random"

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
    PISCUM_API_URL = "https://picsum.photos"

    def get_content(self):
        image_url = f"{self.PISCUM_API_URL}/{self.WIDTH}/{self.HEIGHT}"
        return image_url


class TeamsMessageSender:
    MESSAGE_COLOR = "#F8C471"
    
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


class MessageSenderExecutor:
    def __init__(self, quote_generator, image_generator, message_sender):
        self.quote_generator = quote_generator
        self.image_generator = image_generator
        self.message_sender = message_sender

    def run_process(self):
        quote_content = self.quote_generator.get_content()
        if not quote_content:
            logging.error("Failed to generate quote")
            return None

        quote_text, quote_author = quote_content

        image_url = self.image_generator.get_content()
        if not image_url:
            logging.error("Failed to generate image.")

        self.message_sender.send_message(
            "Sent by Natalia Ananeva",
            quote_text,
            quote_author,
            image_url
        )


# Create instances
quote_generator = QuoteGeneratorFromQuotableAPI()
image_generator = ImageGeneratorFromPiscumAPI()
message_sender = TeamsMessageSender(TEAMS_WEBHOOK_URL)

# Run process
executor = MessageSenderExecutor(
    quote_generator,
    image_generator,
    message_sender
)
executor.run_process()
