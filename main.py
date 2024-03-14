import json
from datetime import datetime

from dotenv import load_dotenv
from time import sleep

from google.cloud import pubsub_v1
import os

from message import near_message
from query_pg import get_contract_addresses, get_token_details, pg_connect, near_query
from util import partition_array

load_dotenv()
# Also need GOOGLE_APPLICATION_CREDENTIALS in env.
project_id = os.environ.get("GCP_PROJECT_ID")
topic_id = os.environ.get("GCP_TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def publish_message(data: str):
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    publisher.publish(topic_path, data)
    # print(f"Published message ID: {future.result()}")


if __name__ == "__main__":
    limit = 20000
    load_dotenv()
    pg_connection = pg_connect()
    tokens = near_query(pg_connection, limit)
    for chunk in partition_array(tokens, 25):
        print(f"Posting {len(chunk)} token requests", datetime.now())
        for a, b, c in chunk:
            message = near_message(a, b, c)
            publish_message(message)
        sleep(1)
