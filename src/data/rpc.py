import sys
import json
import logging
import requests
from pathlib import Path
from sqlalchemy import create_engine, pool, text

sys.path.append(str(Path(__file__).parent.parent))
from data.tracer_config import CUSTOM_TRACER


class XatuClickhouse:
    def __init__(self, db_url, pool_size=5, max_overflow=10, pool_timeout=30):
        self.db_engine = create_engine(
            db_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            poolclass=pool.QueuePool,
        )

    def get_tx_hashes_for_block(
        self,
        block_height: int,
    ) -> list:
        logging.debug(f"Fetching transaction hashes for block {block_height}")
        query = text(
            """
            SELECT transaction_hash
            FROM default.canonical_execution_transaction
            WHERE block_number BETWEEN toUInt64(:start_block) AND toUInt64(:end_block)
                  AND meta_network_name = :network
            ORDER BY block_number ASC, transaction_index ASC
        """
        )
        with self.db_engine.connect() as connection:
            logging.debug(f"Connected to Clickhouse for block {block_height}")
            query_result = connection.execute(
                query,
                {
                    "start_block": block_height,
                    "end_block": block_height,
                    "network": "mainnet",
                },
            )
            logging.debug(f"Query executed for block {block_height}")
            transaction_hashes = [row[0] for row in query_result.fetchall()]
            logging.debug(
                f"Fetched {len(transaction_hashes)} transaction hashes for block {block_height}"
            )
            return transaction_hashes


class ErigonRPC:
    def __init__(
        self,
        erigon_rpc_url: str,
        erigon_rpc_user: str,
        erigon_rpc_pass: str,
        erigon_rpc_response_max_size: int,
    ):
        self.erigon_rpc_url = erigon_rpc_url
        self.erigon_rpc_session = requests.Session()
        self.erigon_rpc_session.auth = (erigon_rpc_user, erigon_rpc_pass)
        self.erigon_rpc_response_max_size = erigon_rpc_response_max_size

    class ResponseTooLargeError(Exception):
        """Custom exception raised when response size exceeds the limit."""

    def _fetch_rpc_response(self, payload: dict) -> str | None:
        """Fetches RPC response from Erigon with error handling and size limits."""
        logging.debug(f"Fetching RPC response with payload: {payload}")
        try:
            response = self.erigon_rpc_session.post(
                self.erigon_rpc_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                stream=True,
            )
            logging.debug(f"RPC request sent to {self.erigon_rpc_url}")
            response.raise_for_status()
            logging.debug("RPC response status OK")
            size = 0
            response_str = ""
            for chunk in response.iter_content(16384):
                size += len(chunk)
                if size > self.erigon_rpc_response_max_size:
                    raise self.ResponseTooLargeError(
                        f"Response size exceeded {self.erigon_rpc_response_max_size} bytes"
                    )
                decoded_chunk = chunk.decode("utf-8", errors="ignore")
                response_str += decoded_chunk
            logging.debug("RPC response fetched and processed successfully")
            return response_str
        except self.ResponseTooLargeError as e:
            logging.error(f"Response too large: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error {e.response.status_code}: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception: {type(e)}: {e}")
            return None

    def _process_trace_response(self, response_str: str) -> list | None:
        """Processes the RPC response string to extract transaction traces."""
        logging.debug("Processing trace response string")
        try:
            trace = json.loads(response_str)
            logging.debug("JSON response loaded successfully")
            if "result" in trace and "data" in trace["result"]:
                data = trace["result"]["data"]
                del trace
                logging.debug("data extracted")
                return data
            else:
                logging.warning("No 'data' found in RPC response.")
                del trace
                return None
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON response.")
            return None

    def fetch_transaction_traces(
        self,
        tx_hash: str,
        block_height: int = 0,
    ) -> list | None:
        """Fetches transaction trace from Erigon RPC."""
        logging.debug(
            f"Fetching transaction trace for tx_hash: {tx_hash}, block_height: {block_height}"
        )
        payload = {
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [tx_hash, {"tracer": CUSTOM_TRACER}],
            "id": 1,
        }
        response_str = self._fetch_rpc_response(payload)
        if not response_str:
            logging.error(
                f"Transaction trace failed for block {block_height}, tx {tx_hash} due to RPC error."
            )
            return [
                {
                    "op": "RPC_FETCH_ERROR",
                    "gas": 0,
                    "gas_cost": 0,
                    "depth": 0,
                    "memory_expansion": 0,
                    "memory_size": 0,
                    "cum_refund": 0,
                    "call_address": "",
                }
            ]
        else:
            data = self._process_trace_response(response_str)
            if data:
                logging.debug(f"Trace fetched for block {block_height}, tx {tx_hash}")
                return data
            else:
                return [
                    {
                        "op": "NO_TRACE",
                        "gas": 0,
                        "gas_cost": 0,
                        "depth": 0,
                        "memory_expansion": 0,
                        "memory_size": 0,
                        "cum_refund": 0,
                        "call_address": "",
                    }
                ]
