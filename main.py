import json
from datetime import datetime

from dotenv import load_dotenv
from time import sleep

from google.cloud import pubsub_v1
import os

from query_pg import get_contract_addresses, get_token_details, pg_connect, near_query

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


def contract_message(contract_address: str) -> str:
    message_dict = {"contract": {"address": contract_address}}
    return json.dumps(message_dict)


def token_message(address: str, token_id: str, token_uri: str) -> str:
    message_dict = {
        "token": {"address": address, "token_id": token_id, "token_uri": token_uri}
    }
    return json.dumps(message_dict)


def near_message(contract_id: str, token_id: str, minter: str) -> str:
    message_dict = {
        "contract_id": contract_id,
        "token_ids": [token_id],
        "minter": minter,
    }
    return json.dumps(message_dict)


def chunk_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


if __name__ == "__main__":
    limit = 300000
    load_dotenv()
    pg_connection = pg_connect()
    tokens = near_query(pg_connection, limit)
    for chunk in chunk_list(tokens, 25):
        print(f"Posting {len(chunk)} token requests", datetime.now())
        for a, b, c in tokens:
            message = near_message(a, b, c)
            publish_message(message)
        sleep(1)
