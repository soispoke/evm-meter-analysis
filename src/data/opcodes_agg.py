import os
import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

pd.options.mode.chained_assignment = None


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
                block_height = block_height_dir_path.split("/")[-3].split("=")[-1]
                print(
                    f"Skipping block {block_height} because no tx_hash directory found inside."
                )
    return block_dirs


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def compute_gas_cost_for_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column to the raw debug trace data (df) with the correct gas cost for the CALL-type opcodes.
    The dataframe can have multiple transactions.
    """
    unique_txs = df["tx_hash"].unique()
    new_df = pd.DataFrame()
    for tx_hash in unique_txs:
        try:
            tx_df = df[df["tx_hash"] == tx_hash]
            new_tx_df = compute_gas_costs_for_single_tx(tx_df)
            new_df = pd.concat([new_df, new_tx_df], ignore_index=True)
        except Exception as e:
            print(f"Error at transactions: {tx_hash}")
            print(f"Exception: {e}")
    return new_df


def compute_gas_costs_for_single_tx(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column to the raw debug trace data (df) with the correct gas cost for the CALL-type opcodes.
    Note that this code assume df only contains data for one transaction
    """
    call_list = ["DELEGATECALL", "CALL", "CALLCODE", "STATICCALL", "CREATE", "CREATE2"]
    inner_gas_diff_list = []
    outer_gas_diff_list = []
    for i in range(len(df) - 1):
        row = df.iloc[i]
        next_row = df.iloc[i + 1]
        # if the row is a new depth starter
        if (row["op"] in call_list) and (next_row["depth"] != row["depth"]):
            # Get the next change of depth that is not a call-type
            return_row = df[
                (df["file_row_number"] > row["file_row_number"])
                & (~df["op"].isin(call_list))
                & (df["depth"] == row["depth"] + 1)
            ].iloc[0]
            # Get and save the available gas at entry and exit
            ## inside the call
            inner_entry_gas = next_row["gas"]
            inner_exit_gas = df[
                df["file_row_number"] == return_row["file_row_number"]
            ].iloc[0]["gas"]
            inner_gas_diff_list.append(inner_entry_gas - inner_exit_gas)
            ## right outside the call
            outer_entry_gas = row["gas"]
            outer_exit_gas = df[
                df["file_row_number"] == return_row["file_row_number"] + 1
            ].iloc[0]["gas"]
            outer_gas_diff_list.append(outer_entry_gas - outer_exit_gas)
        else:
            inner_gas_diff_list.append(np.nan)
            outer_gas_diff_list.append(np.nan)
    # Compute the correct cost for the call opcodes
    call_cost_arr = np.array(outer_gas_diff_list + [np.nan]) - np.array(
        inner_gas_diff_list + [np.nan]
    )
    df["op_gas_cost"] = np.where(df["op"].isin(call_list), call_cost_arr, df["gasCost"])
    return df


def main():
    # Directories
    src_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(src_dir, "..", "..", "data"))
    block_data_dir = os.path.join(data_dir, "block_data")
    block_dirs = get_parquet_path_patterns(block_data_dir)
    # block_dirs now contains paths to parquet files in valid block_height directories
    chunk_size = 5
    block_dirs_chunks = list(chunks(block_dirs, chunk_size))
    total_chunks = len(block_dirs_chunks)
    for dirs_chunk in tqdm(
        block_dirs_chunks, total=total_chunks, desc="Processing chunks"
    ):
        # Define output directory path
        start_block_height = dirs_chunk[0].split("/")[-3].split("=")[-1]
        end_block_height = dirs_chunk[-1].split("/")[-3].split("=")[-1]
        block_range = f"{start_block_height}...{end_block_height}"
        output_dir = os.path.join(
            data_dir,
            "aggregated_opcodes",
            f"block_range={block_range}",
        )
        output_file_path = os.path.join(output_dir, "file.parquet")
        if os.path.isfile(output_file_path):
            print(f"Block range {block_range} is already processed. Skipping...")
            continue
        # Define query
        query = f"""
        SELECT block_height,
            tx_hash,
            file_row_number,
            op,
            gas,
            gasCost,
            depth
        FROM read_parquet(
                { dirs_chunk },
                hive_partitioning = TRUE,
                filename = True,
                file_row_number = True,
                union_by_name = True
            );
        """
        # Execute the query and convert the result to a DataFrame
        raw_df = duckdb.query(query).to_df()
        # Fix issues with gas costs
        clean_df = compute_gas_cost_for_chunk(raw_df)
        # Aggregate data for memory efficiency
        df = (
            clean_df.groupby(["block_height", "tx_hash", "op", "op_gas_cost"])
            .size()
            .reset_index()
        )
        df.columns = [
            "block_height",
            "tx_hash",
            "op",
            "op_gas_cost",
            "op_gas_pair_count",
        ]
        # Save DataFrame to parquet
        os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(output_file_path)


if __name__ == "__main__":
    main()
