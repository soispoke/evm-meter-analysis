setup:
	python -m venv .venv && \
		. .venv/bin/activate && pip install -r requirements.txt

opcodes-raw:
	. .venv/bin/activate && rm -f opcodes_raw_out.log && python src/data/opcodes_raw.py > opcodes_raw_out.log

opcodes-agg:
	. .venv/bin/activate && rm -f opcodes_agg_out.log && python src/opcodes_agg.py > opcodes_agg_out.log