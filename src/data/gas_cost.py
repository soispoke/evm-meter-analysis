import logging
import traceback
import numpy as np
import pandas as pd


def fix_op_gas_cost_for_chunk(df: pd.DataFrame) -> pd.DataFrame:
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
            new_tx_df = fix_op_gas_costs_for_single_tx(tx_df)
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


def fix_op_gas_costs_for_single_tx(initial_df: pd.DataFrame) -> pd.DataFrame:
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


def aggregate_op_gas_cost_data(df: pd.DataFrame) -> pd.DataFrame:
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


def compute_component_gas_costs_per_tx(
    agg_trace_df: pd.DataFrame, tx_gas_info_df: pd.DataFrame, avail_gas_df: pd.DataFrame
) -> pd.DataFrame:
    # Get total cost and input data cost
    comp_df = tx_gas_info_df[
        ["block_height", "tx_hash", "tx_gas_cost", "tx_input_data_cost"]
    ].copy()
    comp_df = comp_df.rename(
        columns={
            "tx_gas_cost": "total_gas_cost",
            "tx_input_data_cost": "input_data_cost",
        }
    )
    # Get base intrinsic cost
    intrinsic_base_df = get_intrinsic_base_cost_per_tx(tx_gas_info_df)
    comp_df = comp_df.merge(intrinsic_base_df, on="tx_hash", how="outer")
    # Get opcode cost
    opcode_df = get_opcode_gas_cost(agg_trace_df)
    comp_df = comp_df.merge(opcode_df, on="tx_hash", how="outer")
    # Get gas refund
    refund_df = get_gas_refunds_per_tx(agg_trace_df)
    comp_df = comp_df.merge(refund_df, on="tx_hash", how="outer")
    # Get intrinsic access cost
    # Currently estimating it from the other components...
    # TODO: Fix the estimation function get_intrinsic_access_cost_per_tx
    comp_df["intrinsic_access_cost"] = (
        comp_df["total_gas_cost"]
        - comp_df["intrinsic_base_cost"]
        - comp_df["input_data_cost"]
        - comp_df["op_gas_cost"]
        + comp_df["gas_refund"]
    )
    return comp_df


def get_intrinsic_base_cost_per_tx(tx_gas_info_df: pd.DataFrame) -> pd.DataFrame:
    df = tx_gas_info_df[["tx_hash", "is_contract_creation"]].copy()
    df["intrinsic_base_cost"] = np.where(
        df["is_contract_creation"] == 1, 53000.0, 21000.0
    )
    df = df.drop(columns="is_contract_creation")
    return df


def get_opcode_gas_cost(agg_trace_df: pd.DataFrame) -> pd.DataFrame:
    df = (
        agg_trace_df.groupby("tx_hash")["op_total_gas_cost"]
        .sum()
        .reset_index(name="op_gas_cost")
    )
    return df


def get_intrinsic_access_cost_per_tx(
    tx_gas_info_df: pd.DataFrame,
    avail_gas_df: pd.DataFrame,
) -> pd.DataFrame:
    df = avail_gas_df.merge(tx_gas_info_df, on="tx_hash", how="inner")
    # Check if tx is simple - an ETH transfer or a call data transaction
    df["is_simple_tx"] = df["tx_gas_cost"] == (21000.0 + df["tx_input_data_cost"])
    # if it simple, no intrinsic access cost. if not simple, compute access cost
    df["intrinsic_access_cost"] = np.where(
        df["is_simple_tx"],
        0.0,
        df["tx_gas_limit"] - df["tx_avail_gas"] - df["tx_input_data_cost"] - 21000.0,
    )
    # futher discount base intrinsic cost if the tx is a contraction creation
    df["intrinsic_access_cost"] = np.where(
        df["is_contract_creation"],
        df["intrinsic_access_cost"] - 32000.0,
        df["intrinsic_access_cost"],
    )
    df = df[["tx_hash", "intrinsic_access_cost"]]
    return df


def get_gas_refunds_per_tx(agg_trace_df: pd.DataFrame) -> pd.DataFrame:
    df = (
        agg_trace_df.groupby("tx_hash")["cum_refund"]
        .max()
        .reset_index(name="gas_refund")
    )
    return df
