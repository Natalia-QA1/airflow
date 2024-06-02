import requests
import random
import pymsteams
import logging
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ContentGenerator(ABC):
    @abstractmethod
    def get_content(self):
        pass


class QuoteGenerator(ContentGenerator):
    def get_content(self):
        """ Method create connection to API and generate random quote"""
        # Make a GET request to the quotable.io API endpoint
        response = requests.get("https://api.quotable.io/random")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()

            # Extract the quote text and author from the response
            quote_text = data["content"]
            quote_author = data["author"]
            logging.info("A quote successfully generated.")

            return quote_text, quote_author
        else:
            logging.error("Failed to retrieve a quote")
            return None, None

class ImageGenerator(ContentGenerator):
    """ Method create connection to API and select random image"""
    def get_content(self):
        width = random.randint(300, 800)
        height = random.randint(300, 800)
        image_url = f"https://picsum.photos/{width}/{height}"
        return image_url


class TeamsMessageSender:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, title, quote_text, quote_author, image_url):
        "Method combines the whole message and send it to MSTeams channel"
        myTeamsMessage = pymsteams.connectorcard(self.webhook_url)
        myTeamsMessage.color("#F8C471")
        myTeamsMessage.title(title)
        myTeamsMessage.text(f"**Quote:** {quote_text}\n\n**Author:** {quote_author}\n\n![Image]({image_url})")
        try:
            myTeamsMessage.send()
            logging.info("Message successfully sent.")
        except Exception as e:
            logging.error("Failed to send message: {e}")


class MessageSenderExecutor:
    def __init__(self, quote_generator, image_generator, message_sender):
        self.quote_generator = quote_generator
        self.image_generator = image_generator
        self.message_sender = message_sender

    def run_process(self):
        quote_text, quote_author = self.quote_generator.get_content()
        if quote_text and quote_author:
            image_url = self.image_generator.get_content()
            if image_url:
                self.message_sender.send_message("Sent by Natalia Ananeva", quote_text, quote_author, image_url)
            else:
                logging.error("Failed to generate image.")
        else:
            logging.error("Failed to generate quote")

# for this simple script
TEAMS_WEBHOOK_URL = "your_webhook"  # TODO: store credentials in secure way

# Create instances
quote_generator = QuoteGenerator()
image_generator = ImageGenerator()
message_sender = TeamsMessageSender(TEAMS_WEBHOOK_URL)

# Run process
executor = MessageSenderExecutor(quote_generator, image_generator, message_sender)
executor.run_process()