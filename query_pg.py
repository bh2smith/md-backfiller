import os
from typing import Any, Optional

import psycopg2


def convert_bytes(memory_view: Any) -> str:
    byte_data = memory_view.tobytes()
    return "0x" + byte_data.hex()


def pg_connect(schema: Optional[str] = None) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(os.environ.get("DB_CONNECTION_STRING"))
    if schema is not None:
        with conn.cursor() as cursor:
            cursor.execute("SET search_path TO mainnet")
    return conn


def get_contract_addresses(
    conn: psycopg2.extensions.connection, limit: int
) -> list[str]:
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


def get_token_details(
    conn: psycopg2.extensions.connection, limit: int
) -> list[(str, str)]:
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


def near_query(
    conn: psycopg2.extensions.connection, limit: int
) -> list[tuple[str, str, str]]:
    query = f"""
        select nft_contract_id, token_id, minter 
        from nft_tokens 
        where metadata_id is null 
        order by minted_timestamp desc 
        nulls last
        limit {limit}
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            print(f"Got {len(rows)} rows")
            return [(row[0], row[1], row[2]) for row in rows]
    except Exception as e:
        print(f"An error occurred: {e}")
