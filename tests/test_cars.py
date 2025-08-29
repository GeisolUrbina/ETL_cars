import sqlite3
import pandas as pd
from pathlib import Path

import etl_cars

def test_transform_cars():
    # Skapa en liten test-DataFrame
    df = pd.DataFrame({
        "Url": ["http://test.se/1"],
        "Registreringsnummer": ["ABC123"],
        "Modellår": [2020],
        "Pris (kr)": [200000],
        "Mätarställning (km)": [5000],
        "Bränsle": ["Bensin"],
        "Biltyp": ["Kombi"],
        "Hästkrafter": [150],
    })

    df_tr = etl_cars.transform_cars(df)

    # Kontrollera kolumner
    assert "price_per_1000km" in df_tr.columns
    # Kontrollera att pris per 1000 km räknas rätt
    assert df_tr.loc[0, "price_per_1000km"] == 40000.0


def test_load_cars(tmp_path):
    # Minimal DataFrame med 1 rad
    df = pd.DataFrame([{
        "url": "http://test.se/1",
        "regnr": "ABC123",
        "model_year": 2020,
        "price_sek": 200000,
        "odometer_km": 5000,
        "fuel": "Bensin",
        "body_type": "Kombi",
        "horsepower": 150,
        "price_per_1000km": 40000.0,
    }])

    # Temporär databasfil
    db_path = tmp_path / "test.db"
    affected = etl_cars.load_cars(df, db_path)

    # Kontrollera att en rad laddades
    assert affected == 1

    # Kontrollera att raden verkligen finns i databasen
    con = sqlite3.connect(db_path)
    row = con.execute("SELECT regnr, price_sek FROM fact_cars").fetchone()
    con.close()

    assert row == ("ABC123", 200000)

