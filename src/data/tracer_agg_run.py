import os
import sys
import duckdb
import logging
import argparse
import pandas as pd
from tqdm import tqdm
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from data.path_mng import (
    chunks,
    get_parquet_path_patterns,
    get_parquet_path_patterns_in_range,
)
from data.gas_cost import compute_gas_cost_for_chunk, aggregate_gas_cost_data


pd.options.mode.chained_assignment = None

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_configuration():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Loads data from EVM debug traces calculates the correct gas costs and aggregates the results."
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=os.path.abspath(os.path.join(file_dir, "..", "..", "data")),
        help="Data directory (default: ./data). Used for both input and output.",
    )
    parser.add_argument(
        "--reprocess",
        type=bool,
        default=False,
        help="Whether to reprocessed blocks. Default is False.",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5,
        help="Number of blocks to process at a time.",
    )
    parser.add_argument(
        "--block_start",
        type=int,
        default=None,
        help="Starting block number. Default is None , i.e., processes the entire raw data",
    )
    parser.add_argument(
        "--block_count",
        type=int,
        default=None,
        help="Number of blocks to process. Default is None , i.e., processes the entire raw data",
    )
    args = parser.parse_args()
    config = {
        "data_dir": args.data_dir,
        "reprocess": args.reprocess,
        "chunk_size": args.chunk_size,
        "block_start": args.block_start,
        "block_count": args.block_count,
    }
    return config


def main():
    config = parse_configuration()
    # Directories
    data_dir = config["data_dir"]
    block_start = config["block_start"]
    block_count = config["block_count"]
    processed_data_dir = os.path.join(data_dir, "aggregated_opcodes_v3")
    raw_data_dir = os.path.join(data_dir, "raw_trace_data")
    if block_start is None or block_count is None:
        block_dirs = get_parquet_path_patterns(raw_data_dir)
    else:
        block_dirs = get_parquet_path_patterns_in_range(
            raw_data_dir, block_start, block_start + block_count
        )
    # Defined processing chunks
    chunk_size = config["chunk_size"]
    block_dirs_chunks = list(chunks(block_dirs, chunk_size))
    total_chunks = len(block_dirs_chunks)
    # Process raw data by chunk
    for dirs_chunk in tqdm(
        block_dirs_chunks, total=total_chunks, desc="Processing chunks"
    ):
        # Define output directory path
        start_block_height = dirs_chunk[0].split("/")[-3].split("=")[-1]
        end_block_height = dirs_chunk[-1].split("/")[-3].split("=")[-1]
        block_range = f"{start_block_height}...{end_block_height}"
        output_dir = os.path.join(processed_data_dir, f"block_range={block_range}")
        output_file_path = os.path.join(output_dir, "file.parquet")
        if os.path.isfile(output_file_path) and config["reprocess"] == False:
            logging.info(f"Block range {block_range} is already processed. Skipping...")
            continue
        logging.info(f"Start processing block range {block_range} ...")
        # Define query
        query = f"""
        SELECT *
        FROM read_parquet(
                { dirs_chunk },
                hive_partitioning = True,
                file_row_number = True,
                union_by_name = True
            );
        """
        # Execute the query and convert the result to a DataFrame
        raw_df = duckdb.query(query).to_df()
        # Fix issues with gas costs
        clean_df = compute_gas_cost_for_chunk(raw_df)
        # Aggregate data for memory efficiency
        df = aggregate_gas_cost_data(clean_df)
        # Save DataFrame to parquet
        os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(output_file_path)


if __name__ == "__main__":
    main()
