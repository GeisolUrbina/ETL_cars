# 🚗 ETL Cars – Excel → SQLite

Detta projekt demonstrerar en **ETL-pipeline** byggd i Python:
- **Extract**: läser data från en Excel-fil (`dataset_final.xlsx`).
- **Transform**: normaliserar kolumner, beräknar pris per 1000 km, rensar dubbletter.
- **Load**: laddar in resultatet i en SQLite-databas (`dataset_final.db`) med upsert-logik.

Projektet är byggt för att köras automatiskt (t.ex. via schemaläggning) och inkluderar loggning och tester.

## 📂 Projektstruktur

```
ETL_cars/
├── etl_cars.py
├── db_cars.py
├── requirements.txt
├── README.md
├── tests/
│   └── test_cars.py
├── data/
│   └── dataset_final.xlsx
└── logs/
    └── app.log
```
