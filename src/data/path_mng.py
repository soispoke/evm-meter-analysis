import os
import logging
from typing import List


def get_block_height_directories(block_data_dir: str) -> List[str]:
    """
    Returns a list of block height directory names found in the given block data directory.
    """
    block_height_directories = sorted(os.listdir(block_data_dir))
    return block_height_directories


def has_tx_hash_directory(block_height_dir_path: str) -> bool:
    """
    Checks if a block height directory contains at least one tx_hash directory.
    """
    for item in os.listdir(block_height_dir_path):
        item_path = os.path.join(block_height_dir_path, item)
        if item.startswith("tx_hash=") and os.path.isdir(item_path):
            return True
    return False


def get_parquet_path_patterns(block_data_dir: str) -> List[str]:
    """
    Generates a list of parquet file path patterns from block height directories containing tx_hash directories.
    """
    block_dirs = []
    block_height_directories = get_block_height_directories(block_data_dir)

    for block_height_dir_name in block_height_directories:
        block_height_dir_path = os.path.join(block_data_dir, block_height_dir_name)
        if block_height_dir_name.startswith("block_height=") and os.path.isdir(
            block_height_dir_path
        ):
            if has_tx_hash_directory(block_height_dir_path):
                block_dirs.append(
                    os.path.join(block_height_dir_path, "tx_hash=*/*.parquet")
                )
            else:
                block_height = block_height_dir_path.split("=")[-1]
                logging.info(
                    f"Skipping block {block_height} because no tx_hash directory found inside."
                )
    return block_dirs


def get_parquet_path_patterns_in_range(
    block_data_dir: str, block_start: int, block_end: int
) -> List[str]:
    """
    Generates a list of parquet file path patterns from block height directories
    containing tx_hash directories. It filters blocks between `block_start` and `block_end`
    """
    block_dirs = []
    block_height_directories = get_block_height_directories(block_data_dir)
    for block_height_dir_name in block_height_directories:
        block_height_dir_path = os.path.join(block_data_dir, block_height_dir_name)
        if block_height_dir_name.startswith("block_height=") and os.path.isdir(
            block_height_dir_path
        ):
            block_height = int(block_height_dir_name.split("=")[-1])
            if (block_height >= block_start) and (block_height <= block_end):
                if has_tx_hash_directory(block_height_dir_path):
                    block_dirs.append(
                        os.path.join(block_height_dir_path, "tx_hash=*/*.parquet")
                    )
                else:
                    block_height = block_height_dir_path.split("=")[-1]
                    logging.info(
                        f"Skipping block {block_height} because no tx_hash directory found inside."
                    )
    return block_dirs


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
