import os
import duckdb
import logging
import argparse
import pandas as pd
from typing import List

pd.options.mode.chained_assignment = None

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def aggregate_and_save_trace_data_for_block_range(
    start_block: int,
    end_block: int,
    input_dir: str,
    output_dir: str,
) -> None:
    logging.info(f"Start processing block range {start_block} .. {end_block}.")
    # Define directories
    input_dirs = os.path.join(input_dir, "block_height=*", "tx_hash=*", "file.parquet")
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
                    '{input_dirs}',
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
    end_chunk_list = list(range(start_block + chunk_size, end_block, chunk_size))
    for chunk_end in end_chunk_list:
        chunk_start = chunk_end - chunk_size
        aggregate_and_save_trace_data_for_block_range(
            chunk_start,
            chunk_end,
            input_data_dir,
            output_data_dir,
        )
    if chunk_end < end_block:
        aggregate_and_save_trace_data_for_block_range(
            chunk_end,
            end_block,
            input_data_dir,
            output_data_dir,
        )


if __name__ == "__main__":
    main()
