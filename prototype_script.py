# sending messages to MS teams channel
#
# 1. using Webhook and pymsteams

"""
Setting Up Your Webhook

Open Microsoft Teams: Log in to your Microsoft Teams account.

Choose a Channel: Select the channel where you want to send messages.
If you don't have a specific channel in mind, you can create a new one.

Configure Incoming Webhook: Click on the ellipsis (...) next to the
channel name and select "Connectors." Search for "Incoming Webhook" and
configure it. Give your webhook a name, upload an image if you like, and
click "Create."

*Copy the Webhook URL*: After creating the webhook, you'll receive a
webhook URL. This URL is essential for sending messages to your Teams
channel. Keep it handy.
"""

import pymsteams

# Initialize the connector card with your webhook URL
myTeamsMessage = pymsteams.connectorcard("YOUR_WEBHOOK_URL_HERE")

# Set the message color
myTeamsMessage.color("#F8C471")

# Add your message text
myTeamsMessage.text("Hello, this is a sample message!")

# Send the message
myTeamsMessage.send()


# 2. using request

import requests
import json

webhook_url = '<your_webhook_url>'

# Define the message
message = {
    "text": "Hello from Python!",
    "title": "Message from Python Script"
}

# Convert the message to JSON
message_json = json.dumps({"text": json.dumps(message)})

# Send the HTTP POST request
response = requests.post(webhook_url, data=message_json, headers={'Content-Type': 'application/json'})

# Check if the request was successful
if response.status_code == 200:
    print("Message sent successfully")
else:
    print("Failed to send message:", response.text)


# get random quote
import requests

def get_random_quote():
    # Make a GET request to the quotable.io API endpoint
    response = requests.get("https://api.quotable.io/random")

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Extract the quote text and author from the response
        quote_text = data["content"]
        quote_author = data["author"]

        # Return the quote text and author
        return quote_text, quote_author
    else:
        # If the request was not successful, print an error message
        print("Error fetching quote:", response.status_code)
        return None


# Get a random quote
result = get_random_quote()

# Check if a quote was successfully retrieved
if result:
    quote_text, quote_author = result

    # Display the quote
    print(quote_text)
    print("- " + quote_author)
else:
    print("Failed to retrieve a quote.")

