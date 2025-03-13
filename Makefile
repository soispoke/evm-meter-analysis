setup:
	python -m venv .venv && \
		. .venv/bin/activate && pip install -r requirements.txt

data: src/data.py
	. .venv/bin/activate && python src/data.py

data-parallel: src/data.py
	. .venv/bin/activate && python src/data-parallel.py
