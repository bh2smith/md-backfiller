import os
from typing import Any

import psycopg2


def convert_bytes(memory_view: Any) -> str:
    byte_data = memory_view.tobytes()
    return "0x" + byte_data.hex()


def connect():
    conn = psycopg2.connect(os.environ.get("DB_CONNECTION_STRING"))
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO mainnet")
    return conn


def get_contract_addresses(limit: int) -> list[str]:
    conn = connect()

    contract_query = (
        f"SELECT address FROM token_contracts WHERE abi_id IS NULL LIMIT {limit};"
    )

    try:
        with conn.cursor() as cur:
            cur.execute(contract_query)
            rows = cur.fetchall()
            return [convert_bytes(row[0]) for row in rows]
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()


def get_token_details(limit: int) -> list[(str, str)]:
    conn = connect()
    contract_query = f"""
        SELECT contract_address, token_id, token_uri
        FROM nfts
        WHERE metadata_id IS NULL
          AND token_uri IS NOT NULL
        LIMIT {limit}"""

    try:
        with conn.cursor() as cur:
            cur.execute(contract_query)
            rows = cur.fetchall()
            return [(convert_bytes(row[0]), str(row[1]), row[2]) for row in rows]
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
