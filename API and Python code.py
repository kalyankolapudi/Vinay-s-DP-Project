https://dummyjson.com/carts?limit=30&utm_source=chatgpt.com

!pip install -q google-cloud-pubsub


import requests
import json
from datetime import datetime, timezone

import google.auth
from google.colab import auth
from google.cloud import pubsub_v1


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

API_URL = "https://dummyjson.com/carts?limit=30"

PROJECT_ID = "dataengineering-major-project"
TOPIC_ID = "ecommerce-orders"


# --------------------------------------------------
# 2. FETCH DATA FROM THE REST API
# --------------------------------------------------

response = requests.get(API_URL, timeout=30)

# Check whether the API request succeeded
response.raise_for_status()

# Convert the API response into Python data
api_data = response.json()

# Extract cart records
carts = api_data.get("carts", [])

if not carts:
    raise ValueError("The API returned no cart records.")

print("API ingestion successful")
print("Records received:", len(carts))


# --------------------------------------------------
# 3. CREATE ONE EVENT FOR EACH CART
# --------------------------------------------------

events = []

for cart in carts:

    event = {
        "event_type": "ecommerce_cart",
        "source": "dummyjson",
        "ingestion_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "data": cart
    }

    events.append(event)

print("Events created:", len(events))

# Preview the first event
print(json.dumps(events[0], indent=2))


# --------------------------------------------------
# 4. AUTHENTICATE WITH GOOGLE CLOUD
# --------------------------------------------------

# Sign in through the Google account prompt
auth.authenticate_user()

# Obtain credentials for Google Cloud APIs
credentials, _ = google.auth.default(
    project=PROJECT_ID
)


# --------------------------------------------------
# 5. CONNECT TO THE PUB/SUB TOPIC
# --------------------------------------------------

publisher = pubsub_v1.PublisherClient(
    credentials=credentials
)

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)

print("Publishing to:", topic_path)


# --------------------------------------------------
# 6. PUBLISH EVENTS TO PUB/SUB
# --------------------------------------------------

published_count = 0

for event in events:

    # Convert the Python dictionary into JSON bytes
    message_data = json.dumps(event).encode("utf-8")

    # Publish the event to the topic
    future = publisher.publish(
        topic_path,
        message_data,
        event_type=event["event_type"],
        source=event["source"]
    )

    # Wait for Pub/Sub to acknowledge the message
    message_id = future.result(timeout=30)

    published_count += 1

    print(
        f"Published event {published_count}: "
        f"message_id={message_id}"
    )


# --------------------------------------------------
# 7. FINAL SUMMARY
# --------------------------------------------------

print("\nIngestion and publishing summary")
print("--------------------------------")
print("Source API:", API_URL)
print("Project ID:", PROJECT_ID)
print("Pub/Sub topic:", TOPIC_ID)
print("API cart records:", len(carts))
print("Events created:", len(events))
print("Messages acknowledged:", published_count)
