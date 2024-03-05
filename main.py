import json
from dotenv import load_dotenv
from time import sleep

from google.cloud import pubsub_v1
import os

from query_pg import get_contract_addresses, get_token_details

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
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")


def contract_message(contract_address: str) -> str:
    message_dict = {"contract": {"address": contract_address}}
    return json.dumps(message_dict)


def token_message(address: str, token_id: str, token_uri: str) -> str:
    message_dict = {
        "token": {"address": address, "token_id": token_id, "token_uri": token_uri}
    }
    return json.dumps(message_dict)


if __name__ == "__main__":
    N = 10

    while 1:
        # Post Contract Stuff:
        print("Contracts")
        contract_addresses = get_contract_addresses(N)
        if len(contract_addresses) < N:
            break

        for address in contract_addresses:
            publish_message(contract_message(address))

        print("Tokens")
        M = 5 * N
        tokens = get_token_details(M)
        if len(tokens) < M:
            break

        for address, t_id, uri in tokens:
            message = token_message(address, t_id, uri)
            publish_message(message)
        sleep(10)
