setup:
	python -m venv .venv && \
		. .venv/bin/activate && pip install -r requirements.txt

opcodes-raw:
	. .venv/bin/activate && python src/data.py > opcodes_raw_out.log

opcodes-agg:
	. .venv/bin/activate && python src/query_duckdb.py > opcodes_agg_out.log