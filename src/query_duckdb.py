import duckdb

con = duckdb.connect()
query = """
    SELECT count(op), op, gasCost,
        regexp_extract(filename, 'block_data/([^/]+)/', 1) as block_number,
        regexp_extract(filename, 'block_data/[^/]+/([^/]+)\\.parquet', 1) as tx_hash
    FROM read_parquet('../data/block_data/*/*.parquet', filename=True)
    GROUP BY op, gasCost, filename;
"""
df = con.execute(query).fetchdf()
df
