import json


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
        "kind": "token",
        "payload": {
            "contract_id": contract_id,
            "token_ids": [token_id],
            "minter": minter,
        },
    }
    return json.dumps(message_dict)
