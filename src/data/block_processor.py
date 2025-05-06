import os
import gc
import sys
import logging
import numpy as np
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from typing import List
from pathlib import Path
from sqlalchemy import text          

sys.path.append(str(Path(__file__).parent.parent))
from data.rpc import XatuClickhouse, ErigonRPC


class BlockProcessor:
    def get_block_dir(self, block_height):
        return os.path.join(self.raw_data_dir, f"block_height={block_height}")
    """Key functionality to process trace data for blocks"""

    # ──────────────────────────────────────────────────────────
    #  internal helper that avoids ClickHouse bind‑param bug
    # ──────────────────────────────────────────────────────────
    def _safe_get_tx_hashes_for_block(self, height: int) -> List[str]:
        """
        Return list of tx hashes for `height`, embedding the block‑number literal
        to avoid the ClickHouse parameter‑escaping bug, and wrapping the SQL in
        sqlalchemy.text so the driver accepts it.
        """
        sql = f"""
            SELECT transaction_hash
            FROM default.canonical_execution_transaction
            WHERE block_number = {height}
              AND meta_network_name = 'mainnet'
            ORDER BY transaction_index ASC
        """
        try:
            with self.xatu_clickhouse_fetcher.db_engine.connect() as conn:
                #   ↓↓↓ wrap with text()  ↓↓↓
                result = conn.execute(text(sql))
                return [row[0] for row in result.fetchall()]
        except Exception as exc:
            logging.error("ClickHouse query failed for block %d: %s", height, exc)
            return []

    # ──────────────────────────────────────────────────────────
    #  rest of __init__ unchanged
    # ──────────────────────────────────────────────────────────
    def __init__(
        self,
        raw_data_dir: str,
        xatu_clickhouse_fetcher: XatuClickhouse,
        erigon_rpc: ErigonRPC,
        thread_pool_size: int = 8,
    ):
        self.raw_data_dir = raw_data_dir
        self.xatu_clickhouse_fetcher = xatu_clickhouse_fetcher
        self.erigon_rpc = erigon_rpc
        self.thread_pool_size = thread_pool_size
    # ---------------------------------------------------------------------

    def get_tx_hashes_to_process(self, block_height: int, reprocess: bool) -> List[str]:
        """
        Return the list of transaction hashes to process.
        If reprocess=False, checks if a block is fully processed or whether there
        are missing transactions.
        """
        # ①  use the safe query instead of the buggy driver call
        transaction_hashes = self._safe_get_tx_hashes_for_block(block_height)

        block_dir = self.get_block_dir(block_height)

        if reprocess:
            logging.info("Reprocessing block %d.", block_height)
            return transaction_hashes

        if not os.path.exists(block_dir):
            logging.info("Block %d not yet processed; processing it now.", block_height)
            return transaction_hashes

        # existing “missing‑file” logic unchanged …
        missing = []
        for txh in transaction_hashes:
            pq = os.path.join(block_dir, f"tx_hash={txh}", "file.parquet")
            if not os.path.exists(pq):
                missing.append(txh)

        if missing:
            logging.info("Block %d missing %d tx parquet(s); re‑processing those.",
                         block_height, len(missing))
        else:
            logging.info("Block %d already fully processed; skipping.", block_height)
        return missing_tx_hashes

    def _write_traces_to_parquet(self, traces, block_height, tx_hash):
        """Writes transaction traces to Parquet file."""
        logging.debug(
            f"Writing traces to Parquet for block {block_height}, tx_hash: {tx_hash}"
        )
        block_dir = self.get_block_dir(block_height)
        tx_dir = os.path.join(block_dir, f"tx_hash={tx_hash}")
        os.makedirs(tx_dir, exist_ok=True)
        parquet_file_path = os.path.join(tx_dir, "file.parquet")
        try:
            df = pd.DataFrame(traces)
            df.to_parquet(parquet_file_path)
            logging.debug(
                f"Traces written to Parquet for block {block_height}, tx_hash: {tx_hash}"
            )
        except Exception as e:
            logging.error(
                f"Failed to write traces to Parquet: {parquet_file_path}. Error: {e}"
            )
            return
        logging.debug(
            f"Transaction traces written to Parquet file: {parquet_file_path}"
        )

    def _fetch_transaction(self, tx_hash, block_height) -> pd.DataFrame:
        """Fetches the trace of  single transaction and returns as DataFrame"""
        logging.debug(f"Fetching trace for tx_hash: {tx_hash} in block {block_height}")
        traces = self.erigon_rpc.fetch_transaction_traces(
            tx_hash,
            block_height=block_height,
        )
        try:
            df = pd.DataFrame(traces)
            logging.debug(
                f"Manage to load trace for block {block_height}, tx_hash: {tx_hash}"
            )
        except Exception as e:
            logging.error(
                f"Failed to load trace for block {block_height}, tx_hash: {tx_hash}. Error: {e}"
            )
            return
        gc.collect()
        df["file_row_number"] = np.arange(len(df))
        df["tx_hash"] = tx_hash
        return df

    def _fetch_and_save_transaction(self, tx_hash, block_height):
        """Processes a single transaction: fetches trace and writes to parquet."""
        logging.debug(f"Fetching trace for tx_hash: {tx_hash} in block {block_height}")
        traces = self.erigon_rpc.fetch_transaction_traces(
            tx_hash,
            block_height=block_height,
        )
        self._write_traces_to_parquet(traces, block_height, tx_hash)
        gc.collect()

    def fetch_and_save_block_range(
        self,
        block_start: int,
        block_count: int,
        reprocess: bool,
    ):
        """Processes a range of blocks to fetch and stores the transaction traces."""
        logging.debug(
            f"Processing block range from {block_start} to {block_start + block_count - 1}"
        )
        for block_height in tqdm(range(block_start, block_start + block_count)):
            logging.debug(f"Processing block {block_height}")
            tx_hashes_to_process = self.get_tx_hashes_to_process(
                block_height, reprocess
            )
            if len(tx_hashes_to_process) == 0:
                continue
            else:
                block_dir = self.get_block_dir(block_height)
                if not os.path.exists(block_dir):
                    os.makedirs(block_dir, exist_ok=True)
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.thread_pool_size
                ) as executor:
                    futures = [
                        executor.submit(
                            self._fetch_and_save_transaction, tx_hash, block_height
                        )
                        for tx_hash in tx_hashes_to_process
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                logging.debug(f"Finished processing block {block_height}")
            logging.debug("Finished processing block range")

    def fetch_block(
        self,
        block_height: int,
    ) -> pd.DataFrame:
        """fetches trace data for single block"""
        logging.debug(f"Processing block {block_height}")
        tx_hashes_to_process = self.get_tx_hashes_to_process(block_height, True)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.thread_pool_size
        ) as executor:
            futures = [
                executor.submit(self._fetch_transaction, tx_hash, block_height)
                for tx_hash in tx_hashes_to_process
            ]
            df_list = []
            for future in concurrent.futures.as_completed(futures):
                df_list.append(future.result())
        df = pd.concat(df_list, ignore_index=True)
        logging.debug(f"Finished processing block {block_height}")
        return df
