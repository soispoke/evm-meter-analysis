import logging
import traceback
import numpy as np
import pandas as pd


def compute_gas_cost_for_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column named `op_gas_cost` to df with the correct gas cost for the CALL-type opcodes
    and contract creation opcodes. df must have at least the following columns: op, gas, gas_cost,
    depth, file_row_number, tx_hash
    The dataframe can have multiple transactions.
    """
    unique_txs = df["tx_hash"].unique()
    new_df = pd.DataFrame()
    for tx_hash in unique_txs:
        tx_df = df[df["tx_hash"] == tx_hash]
        try:
            new_tx_df = compute_gas_costs_for_single_tx(tx_df)
        except:
            logging.error(f"Error at transaction: {tx_hash}")
            logging.error("Traceback:")
            traceback.print_exc()
            new_tx_df = tx_df.copy()
            new_tx_df["op_gas_cost"] = new_tx_df["gas_cost"]
            logging.error(
                "The column op_gas_cost will be set to original gas_cost. Continuing to process chunk."
            )
        new_df = pd.concat([new_df, new_tx_df], ignore_index=True)
    return new_df


def compute_gas_costs_for_single_tx(initial_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column named `op_gas_cost` to initial_df with the correct gas cost for the
    CALL-type opcodes and contract creation opcodes. initial_df must have at least the following
    columns: op, gas, gas_cost, depth, file_row_number
    Note that this code assume df only contains data for one transaction.
    """
    df = initial_df.copy()
    if df["depth"].nunique() == 1:  # if transaction does not have depth changes
        # We only need to fix calls
        df["op_gas_cost"] = np.where(
            df["op"].isin(["DELEGATECALL", "STATICCALL", "CALL", "CALLCODE"]),
            df["gas"] - df["gas"].shift(-1),
            df["gas_cost"],
        )
        return df
    else:  # if the transaction has depth changes...
        df["has_depth_increase"] = df["depth"] < df["depth"].shift(-1)
        df["has_depth_decrease"] = df["depth"] > df["depth"].shift(-1)
        df["has_depth_change"] = df["depth"] != df["depth"].shift(-1)
        inner_gas_diff_list = []
        outer_gas_diff_list = []
        change_depth_row_numbers = df[df["has_depth_change"]]["file_row_number"].values
        filter_row_numbers = np.unique(
            np.concat(
                (
                    change_depth_row_numbers,
                    change_depth_row_numbers + 1,
                    change_depth_row_numbers - 1,
                )
            )
        )
        filter_rows_df = df[df["file_row_number"].isin(filter_row_numbers)].sort_values(
            "file_row_number"
        )
        for i in range(len(filter_rows_df) - 1):
            row = filter_rows_df.iloc[i]
            next_row = filter_rows_df.iloc[i + 1]
            # if the row is a new depth starter
            if row["has_depth_increase"]:
                # Get the next row that decreases the depth
                return_row = filter_rows_df[
                    (filter_rows_df["file_row_number"] > row["file_row_number"])
                    & (filter_rows_df["has_depth_decrease"])
                    & (filter_rows_df["depth"] == row["depth"] + 1)
                ].iloc[0]
                # Get and save the available gas at entry and exit
                ## inside the call
                inner_entry_gas = next_row["gas"]
                inner_exit_gas = filter_rows_df[
                    filter_rows_df["file_row_number"] == return_row["file_row_number"]
                ].iloc[0]["gas"]
                inner_gas_diff_list.append(inner_entry_gas - inner_exit_gas)
                ## right outside the call
                outer_entry_gas = row["gas"]
                outer_exit_gas = filter_rows_df[
                    filter_rows_df["file_row_number"]
                    == return_row["file_row_number"] + 1
                ].iloc[0]["gas"]
                outer_gas_diff_list.append(outer_entry_gas - outer_exit_gas)
            else:
                inner_gas_diff_list.append(np.nan)
                outer_gas_diff_list.append(np.nan)
        # Compute the correct cost for the depth starter rows
        depth_starter_cost_df = pd.DataFrame(
            {
                "file_row_number": filter_rows_df["file_row_number"].values,
                "op_gas_cost": np.array(outer_gas_diff_list + [np.nan])
                - np.array(inner_gas_diff_list + [np.nan]),
            }
        )
        # Merge with original df
        df = df.merge(depth_starter_cost_df, on="file_row_number", how="left").drop(
            columns=["has_depth_change", "has_depth_increase", "has_depth_decrease"]
        )
        df["op_gas_cost"] = np.where(
            df["op_gas_cost"].isna(), df["gas_cost"], df["op_gas_cost"]
        )
        # Now, we only need to fix calls of same depth
        df["op_gas_cost"] = np.where(
            (df["op"].isin(["DELEGATECALL", "STATICCALL", "CALL", "CALLCODE"]))
            & (df["depth"] == df["depth"].shift(-1)),
            df["gas"] - df["gas"].shift(-1),
            df["op_gas_cost"],
        )
        return df


def aggregate_gas_cost_data(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "block_height",
        "tx_hash",
        "op",
        "op_gas_cost",
        "memory_expansion",
        "memory_size",
        "cum_refund",
        "call_address",
    ]
    groupby_cols = []
    for col in base_cols:
        if col in df.columns:
            groupby_cols.append(col)
    agg_df = df.groupby(groupby_cols).size().reset_index()
    agg_df = agg_df.rename(columns={0: "op_gas_pair_count"})
    return agg_df
