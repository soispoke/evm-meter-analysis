import os
import sys
import duckdb
import logging
import argparse
import pandas as pd
from typing import List
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from data.path_mng import chunks, get_parquet_path_patterns_in_range

pd.options.mode.chained_assignment = None

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def aggregate_and_save_trace_data_for_dirs_chunk(
    dirs_chunk: List[str],
    output_dir: str,
) -> None:
    # Get block ranges
    start_block = dirs_chunk[0].split("/")[-3].split("=")[-1]
    end_block = dirs_chunk[-1].split("/")[-3].split("=")[-1]
    logging.info(f"Start processing block range {start_block} .. {end_block}.")
    # Define output directory and file
    block_range_output_dir = os.path.join(
        output_dir, f"block_range={start_block}...{end_block}"
    )
    os.makedirs(block_range_output_dir, exist_ok=True)
    output_file = os.path.join(block_range_output_dir, "file.parquet")
    # Define query
    query = f"""
    COPY
        (WITH gas_cost_calc AS (
                SELECT
                    *,
                    CASE
                        WHEN op IN ('DELEGATECALL', 'CALL', 'CALLCODE', 'STATICCALL')
                            AND depth < LEAD(depth, 1, depth) OVER ()
                        THEN gasCost - LEAD(gas, 1, 0) OVER ()
                        WHEN op IN ('DELEGATECALL', 'CALL', 'CALLCODE', 'STATICCALL')
                            AND depth = LEAD(depth, 1, depth) OVER ()
                        THEN gas - LEAD(gas, 1, 0) OVER ()
                        ELSE gasCost
                    END AS gasCost_v2
                FROM read_parquet(
                    { dirs_chunk },
                    hive_partitioning = TRUE,
                    filename = True,
                    file_row_number = True,
                    union_by_name = True
                )
                WHERE block_height BETWEEN {start_block} AND {end_block}
            )
        SELECT
            block_height, 
            tx_hash, 
            op, 
            gasCost_v2 AS op_gas_cost, 
            COUNT(op) AS op_gas_pair_count
        FROM gas_cost_calc
        GROUP BY
            op,
            gasCost_v2,
            block_height,
            tx_hash)
    TO '{output_file}' (FORMAT PARQUET);
    """
    # Execute the query and save to parquet
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute(query)
    duckdb_conn.close()


def parse_configuration():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Loads data from EVM debug traces calculates the correct gas costs and aggregates the results."
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=os.path.abspath(os.path.join(src_dir, "..", "..", "data")),
        help="Data directory (default: ./data). Used for both input and output.",
    )
    parser.add_argument(
        "--start_block",
        type=int,
        default=22_000_020,
        help="Start block to process. Defaults to 22,000,020.",
    )
    parser.add_argument(
        "--end_block",
        type=int,
        default=22_006_020,
        help="End block to process. Defaults to 22,006,020.",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5,
        help="Number of block to process at a time. Higher chunk size will lead to less checkpointing.",
    )
    args = parser.parse_args()
    config = {
        "data_dir": args.data_dir,
        "start_block": args.start_block,
        "end_block": args.end_block,
        "chunk_size": args.chunk_size,
    }
    return config


def main() -> None:
    config = parse_configuration()
    # Directories
    data_dir = config["data_dir"]
    input_data_dir = os.path.join(data_dir, "block_data")
    output_data_dir = os.path.join(data_dir, "aggregated_opcodes_v1")
    os.makedirs(output_data_dir, exist_ok=True)
    # Process blocks by chunk
    start_block = config["start_block"]
    end_block = config["end_block"]
    chunk_size = config["chunk_size"]
    block_dirs = get_parquet_path_patterns_in_range(
        input_data_dir, start_block, end_block
    )
    block_dirs_chunks = list(chunks(block_dirs, chunk_size))
    for dirs_chunk in block_dirs_chunks:
        aggregate_and_save_trace_data_for_dirs_chunk(
            dirs_chunk,
            output_data_dir,
        )


if __name__ == "__main__":
    main()
