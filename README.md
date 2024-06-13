

python -m create_dl.py

../../bin/duckdb

SELECT *
FROM delta_scan('./my_delta_table')
ORDER BY id;