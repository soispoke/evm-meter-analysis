import os
import sys
import json
import logging
import argparse
import numpy as np
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from data.rpc import XatuClickhouse, ErigonRPC
from data.block_processor import BlockProcessor
from data.gas_cost import fix_op_gas_cost_for_chunk, aggregate_op_gas_cost_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_configuration():
    """
    Parses command line arguments and secrets, and returns a configuration dictionary.
    """
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Process a sample of Ethereum blocks and transaction traces and saves them to parquet files."
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=os.path.abspath(os.path.join(file_dir, "..", "..", "data")),
        help="Data directory (default: ./data). Parquet files will be stored in a folder there.",
    )
    parser.add_argument(
        "--block_start",
        type=int,
        default=22000000,
        help="Starting block number (default: 22000000)",
    )
    parser.add_argument(
        "--block_end",
        type=int,
        default=22300000,
        help="Ending block number (default: 22300000)",
    )
    parser.add_argument(
        "--block_count",
        type=int,
        default=6000,
        help="Number of blocks to sample between block_start and block_end (default: 6000)",
    )
    parser.add_argument(
        "--thread_pool_size",
        type=int,
        default=8,
        help="Number of threads to use for processing transactions (default: 8)",
    )
    parser.add_argument(
        "--clickhouse_pool_size",
        type=int,
        default=5,
        help="Clickhouse connection pool size (default: 5)",
    )
    parser.add_argument(
        "--clickhouse_max_overflow",
        type=int,
        default=10,
        help="Clickhouse connection pool max overflow (default: 10)",
    )
    parser.add_argument(
        "--clickhouse_pool_timeout",
        type=int,
        default=30,
        help="Clickhouse connection pool timeout (default: 30)",
    )
    parser.add_argument(
        "--secrets_path",
        type=str,
        default=os.path.abspath(os.path.join(file_dir, "..", "..", "secrets.json")),
        help="Path to secrets.json file (default: ./secrets.json)",
    )
    parser.add_argument(
        "--erigon_rpc_url",
        type=str,
        default="https://rpc-mainnet-teku-erigon-001.utility.production.platform.ethpandaops.io",
        help="Erigon RPC URL (default: https://rpc-mainnet-teku-erigon-001.utility.production.platform.ethpandaops.io)",
    )
    parser.add_argument(
        "--erigon_username",
        type=str,
        help="Erigon RPC username (can be provided in secrets.json)",
    )
    parser.add_argument(
        "--erigon_password",
        type=str,
        help="Erigon RPC password (can be provided in secrets.json)",
    )
    parser.add_argument(
        "--erigon_rpc_response_max_size",
        type=int,
        default=int(1e9),
        help="Maximum response size for Erigon RPC calls (default: 1GB)",
    )
    parser.add_argument(
        "--xatu_clickhouse_url_base",
        type=str,
        default="clickhouse+http://clickhouse.xatu.ethpandaops.io:443/default?protocol=https",
        help="Clickhouse URL base (default: clickhouse+http://clickhouse.xatu.ethpandaops.io:443/default?protocol=https)",
    )
    parser.add_argument(
        "--xatu_username",
        type=str,
        help="Xatu Clickhouse username (can be provided in secrets.json)",
    )
    parser.add_argument(
        "--xatu_password",
        type=str,
        help="Xatu Clickhouse password (can be provided in secrets.json)",
    )
    args = parser.parse_args()
    config = {}
    config["data_dir"] = args.data_dir
    config["secrets_path"] = args.secrets_path
    config["block_start"] = args.block_start
    config["block_end"] = args.block_end
    config["block_count"] = args.block_count
    config["thread_pool_size"] = args.thread_pool_size
    config["clickhouse_pool_size"] = args.clickhouse_pool_size
    config["clickhouse_max_overflow"] = args.clickhouse_max_overflow
    config["clickhouse_pool_timeout"] = args.clickhouse_pool_timeout
    config["xatu_clickhouse_url_base"] = args.xatu_clickhouse_url_base
    config["xatu_username"] = args.xatu_username
    config["xatu_password"] = args.xatu_password
    config["erigon_rpc_url"] = args.erigon_rpc_url
    config["erigon_username"] = args.erigon_username
    config["erigon_password"] = args.erigon_password
    config["erigon_rpc_response_max_size"] = args.erigon_rpc_response_max_size
    try:
        with open(config["secrets_path"], "r") as file:
            secrets_dict = json.load(file)
        logging.debug(f"Secrets loaded from {config['secrets_path']}")
        if not config["xatu_username"]:
            config["xatu_username"] = secrets_dict.get("xatu_username")
        if not config["xatu_password"]:
            config["xatu_password"] = secrets_dict.get("xatu_password")
        if not config["erigon_username"]:
            config["erigon_username"] = secrets_dict.get("erigon_username")
        if not config["erigon_password"]:
            config["erigon_password"] = secrets_dict.get("erigon_password")
    except FileNotFoundError:
        logging.warning(
            f"Secrets file not found at {config['secrets_path']}. Secrets might be missing if not provided via command line."
        )
    return config


def main():
    logging.debug("Starting main function")
    # Config
    config = parse_configuration()
    # Directories
    data_dir = config["data_dir"]
    save_data_dir = os.path.join(data_dir, "aggregated_opcodes_sample")
    os.makedirs(save_data_dir, exist_ok=True)
    logging.debug(f"Output directory created or exists: {save_data_dir}")
    # Xatur clickhouse config
    thread_pool_size = config["thread_pool_size"]
    clickhouse_pool_size = config["clickhouse_pool_size"]
    clickhouse_max_overflow = config["clickhouse_max_overflow"]
    clickhouse_pool_timeout = config["clickhouse_pool_timeout"]
    xatu_username = config.get("xatu_username")
    xatu_password = config.get("xatu_password")
    if not xatu_username or not xatu_password:
        logging.error(
            "Xatu Clickhouse credentials not found. Please provide them via command line, environment variables, or secrets.json."
        )
        return
    xatu_clickhouse_url_base = config["xatu_clickhouse_url_base"]
    db_url = f"{xatu_clickhouse_url_base.split('://', 1)[0]}://{xatu_username}:{xatu_password}@{xatu_clickhouse_url_base.split('://', 1)[1]}"
    xatu_clickhouse_fetcher = XatuClickhouse(
        db_url,
        pool_size=clickhouse_pool_size,
        max_overflow=clickhouse_max_overflow,
        pool_timeout=clickhouse_pool_timeout,
    )
    logging.debug("XatuClickhouse instance created")
    # Erigon RPC config
    erigon_rpc_url = config["erigon_rpc_url"]
    erigon_rpc_response_max_size = config["erigon_rpc_response_max_size"]
    erigon_username = config.get("erigon_username")
    erigon_password = config.get("erigon_password")
    if not erigon_username or not erigon_password:
        logging.error(
            "Erigon RPC credentials not found. Please provide them via command line, environment variables, or secrets.json."
        )
        return
    erigon_rpc = ErigonRPC(
        erigon_rpc_url, erigon_username, erigon_password, erigon_rpc_response_max_size
    )
    logging.debug("ErigonRPC instance created")
    # Block processor config
    block_processor = BlockProcessor(
        save_data_dir, xatu_clickhouse_fetcher, erigon_rpc, thread_pool_size
    )
    # Sample blocks to process
    block_start = config["block_start"]
    block_end = config["block_end"]
    block_count = config["block_count"]
    sampled_block_heights = np.random.randint(block_start, block_end, 2)
    sampled_block_heights.sort()
    sampled_block_heights = sampled_block_heights.tolist()
    # Process sampled blocks
    for block_height in sampled_block_heights:
        logging.debug(f"Processing block {block_height}")
        # Fetch raw data from debug traces
        raw_df = block_processor.fetch_block(block_height)
        # Fix issues with gas costs
        clean_df = fix_op_gas_cost_for_chunk(raw_df)
        # Aggregate data for memory efficiency
        df = aggregate_op_gas_cost_data(clean_df)
        logging.debug(f"Finished to process block {block_height}")
        # Save as parquet
        try:
            save_dir = block_processor.get_block_dir(block_height)
            os.makedirs(save_dir, exist_ok=True)
            parquet_file_path = os.path.join(save_dir, "file.parquet")
            df.to_parquet(parquet_file_path, index=False)
            logging.debug(f"Saved data for block {block_height}")
        except Exception as e:
            logging.error(
                f"Failed to write traces to Parquet: {parquet_file_path}. Error: {e}"
            )
            continue
    logging.debug("Ending main function")


if __name__ == "__main__":
    main()
