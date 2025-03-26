import duckdb
import glob
import pandas as pd
from tqdm import tqdm
import os


def get_block_height_directories(block_data_dir):
    """
    Returns a list of block height directory names found in the given block data directory.
    """
    block_height_directories = sorted(os.listdir(block_data_dir))
    return block_height_directories


def has_tx_hash_directory(block_height_dir_path):
    """
    Checks if a block height directory contains at least one tx_hash directory.
    """
    for item in os.listdir(block_height_dir_path):
        item_path = os.path.join(block_height_dir_path, item)
        if item.startswith("tx_hash=") and os.path.isdir(item_path):
            return True
    return False


def get_parquet_path_patterns(block_data_dir):
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
                print(
                    f"Skipping {block_height_dir_path} because no tx_hash directory found inside."
                )
    return block_dirs


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main():
    block_data_dir = "../data/block_data/"
    block_dirs = get_parquet_path_patterns(block_data_dir)
    # block_dirs now contains paths to parquet files in valid block_height directories
    chunk_size = 5
    block_dirs_chunks = list(chunks(block_dirs, chunk_size))
    total_chunks = len(block_dirs_chunks)
    for dirs_chunk in tqdm(
        block_dirs_chunks, total=total_chunks, desc="Processing chunks"
    ):
        query = f"""
        WITH gas_cost_calc AS (
                SELECT
                    *,
                    CASE
                        WHEN op IN ('DELEGATECALL', 'CALL', 'CALLCODE', 'STATICCALL')
                            AND depth <> LEAD(depth, 1, depth) OVER ()
                        THEN gasCost - LEAD(gas, 1, 0) OVER ()
                        WHEN op IN ('DELEGATECALL', 'CALL', 'CALLCODE', 'STATICCALL')
                            AND depth = LEAD(depth, 1, depth) OVER ()
                        THEN gas - LEAD(gas, 1, 0) OVER ()
                        ELSE gasCost
                    END AS gasCost_v2
                FROM read_parquet({dirs_chunk},
                    hive_partitioning=TRUE,
                    filename=True
                )
            )
        SELECT
            COUNT(op) AS op_count,
            op,
            gasCost_v2,
            block_height,
            tx_hash
        FROM gas_cost_calc
        GROUP BY
            op,
            gasCost_v2,
            block_height,
            tx_hash;
        """
        # Execute the query and convert the result to a DataFrame
        df = duckdb.query(query).to_df()
        # Determine block range for output directory
        start_block_height = df["block_height"].min()
        end_block_height = df["block_height"].max()
        block_range = f"{start_block_height}...{end_block_height}"
        # Define output directory path
        output_dir = (
            f"../data/processed_data/aggregated_opcodes/block_range={block_range}"
        )
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, "file.parquet")
        # Save DataFrame to parquet
        df.to_parquet(output_file_path)


if __name__ == "__main__":
    main()
