from typing import Any


def partition_array(lst: list[Any], chunk_size: int) -> list[list[Any]]:
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
