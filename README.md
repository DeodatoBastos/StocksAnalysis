# StocksAnalysis
Final project of CSI-30 Course.

1. Run/Create Docker Container:
```bash
docker compose up -d
```

2. Create Table with `create_tables.sql` file

3. Download Data:
```bash
python3 collector.py
```

4. Populate Tables:
```bash
python3 create_dw.py
```
