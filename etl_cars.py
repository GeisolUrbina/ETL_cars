
from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Tuple, Any, Iterable
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import db_cars

# Modulens logger
LOGGER = logging.getLogger("etl_cars")


# Hjälpfunktioner: loggning

def _setup_logging(log_path: Path, level: int = logging.INFO) -> None:
    """Initiera fil- och konsolloggning."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    if not any(isinstance(h, RotatingFileHandler) for h in LOGGER.handlers):
        file_handler = RotatingFileHandler(
            log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        LOGGER.addHandler(file_handler)

    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in LOGGER.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fmt)
        LOGGER.addHandler(stream_handler)


# Extrahera


def extract_excel(path: Path, sheet: str | int | None = None) -> pd.DataFrame:
    """Läs Excel-fil till DataFrame."""
    LOGGER.info("Extract: läser Excel %s (sheet=%s)", path, sheet)
    try:
        #Läser in Excelfilen om inget blad anges tas det första
        df = pd.read_excel(path, sheet_name=sheet) #path: sökväg till excel-fil, sheet:bladnamn eller index
        # Om pd returnerar en dict vid flera blad plockas det första bladet
        if not isinstance(df, pd.DataFrame):
            df = next(iter(df.values()))
        return df
    
    except FileNotFoundError:
        # Om filen inte finns då logga fel och kasta vidare
        LOGGER.error("Excel-filen hittades inte: %s", path)
        raise
    except Exception as exc:
        #Fångar andra fel och loggar hela stacktracen
        LOGGER.exception("Kunde inte läsa Excel: %s", exc)
        raise



# Transform

# Mappning från kolumnrubriker i excel till standardiserade namn i databasen
COLUMN_MAP = {
    "Url": "url",
    "Registreringsnummer": "regnr",
    "Modellår": "model_year",
    "Pris (kr)": "price_sek",
    "Mätarställning (km)": "odometer_km",
    "Bränsle": "fuel",
    "Biltyp": "body_type",
    "Hästkrafter": "horsepower",
}

# Numeriska kolumner konverteras till taltyper
_NUMERIC_COLS = {"model_year", "price_sek", "odometer_km", "horsepower"}

# Hämtar en kolumn från df om den finns, annars returneras en serie fylld med NA. 
def _safe_get(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([pd.NA] * len(df))


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    
    # Byt ut komlumnnamn enligt vår map
    for src, dest in COLUMN_MAP.items():
        out[dest] = _safe_get(df, src)
    
    #Ersätt tomma strängar med NA    
    for s in ["url", "regnr", "fuel", "body_type"]:
        out[s] = out[s].astype("string").str.strip().replace({"": pd.NA})
        
    #Konvertera numeriska fält till tal    
    for ncol in _NUMERIC_COLS:
        out[ncol] = pd.to_numeric(out[ncol], errors="coerce")
        out[ncol] = out[ncol].astype("Int64")

    return out


def transform_cars(df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info("Transform: normaliserar kolumner och beräknar fält")

    out = _normalize_columns(df)
    
    #Priset och mätarställning som flyttal för beräkning
    price = out["price_sek"].astype("float")
    odo = out["odometer_km"].astype("float")
    
    #Beräkna pris per 1000 km och undvik division med noll och NAN
    with np.errstate(divide="ignore", invalid="ignore"):
        p_per_1000 = (price / odo) * 1000.0
    invalid = (price.isna()) | (odo.isna()) | (odo == 0)
    p_per_1000 = p_per_1000.mask(invalid)
    out["price_per_1000km"] = p_per_1000.round(2)
    
    #Filtrera bort rader utan URL
    out = out[out["url"].notna()]
    
    #Ta bort dubbletter på URL och behåll sista
    before = len(out)
    out = out.drop_duplicates(subset=["url"], keep="last")
    removed = before - len(out)
    if removed:
        LOGGER.info("Transform: %d dubbletter (url) borttagna", removed)
    
    #Återsäll index så raderna blir 0..n    
    out.reset_index(drop=True, inplace=True)
    return out



# Ladda till databasen

def _convert_row_for_db(rows: Iterable[pd.Series]) -> List[Tuple[Any, ...]]:
    out_rows: List[Tuple[Any, ...]] = []
    now = datetime.now(timezone.utc).isoformat() #tidsstämpel i ISO-format (UTC)

    for r in rows:
        # Hjälpfunktion: om värdet är NaN eller None då returnera None,
        # annars returnera värdet, ev. typkonverterat med cast
        def _none(v: Any, cast: Any | None = None):
            if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v):
                return None
            return cast(v) if cast else v
        
        #Bygger upp en tupel med fälten i rätt ordning som matchar tabellen i databasen.

        out_rows.append(
            (
                _none(r[0], str), # url
                _none(r[1], str), # registreringsnummer
                _none(r[2], int), # modellår
                _none(r[3], int), # pris (SEK)
                _none(r[4], int), # mätarställning (km)
                _none(r[5], str), # bränsle
                _none(r[6], str), # biltyp
                _none(r[7], int), # hästkrafter
                _none(r[8], float), # pris per 1000 km
                now,                # tidsstämpel för ETL-laddningen
            )
        )

    return out_rows

#Laddar in transformad df till SQLite-databasen
def load_cars(df: pd.DataFrame, db_path: Path) -> int:
    LOGGER.info("Load: upsert till SQLite %s", db_path)
    conn = db_cars.get_conn(db_path) # anslutning till databasen
    try:
        db_cars.init_schema(conn) # säkerställ att tabellen finns 
        rows = _convert_row_for_db(df.itertuples(index=False, name=None)) #Konvertera rader
        affected = db_cars.upsert_cars(conn, rows) #infoga coh uppdatera rader
        LOGGER.info("Load: %d rader upsertade.", affected)
        return affected
    finally:
        conn.close() #stäng anslutningen 



# Orkestrering / CLI
# Den som kör hela ETL-processen

def run_etl(excel_path: Path, db_path: Path, log_path: Path, sheet: str | int | None = None) -> int:
    _setup_logging(log_path)
    try:
        LOGGER.info("=== CARS ETL START ===")
        LOGGER.info("Excel: %s | DB: %s | Logg: %s", excel_path, db_path, log_path)
        
        # 1. Läs rådata från Excel-fil
        df_raw = extract_excel(excel_path, sheet=sheet)
        # 2. Transformera data
        df_tr = transform_cars(df_raw)
        # 3. Ladda data till SQLite
        affected = load_cars(df_tr, db_path)
        
        LOGGER.info("=== CARS ETL KLAR: %d rader ===", affected)
        return affected
    except Exception as e:
        LOGGER.exception("CARS ETL misslyckades: %s", e)
        raise

# Funktion för att tolka argumentet
def _sheet_arg(value: str) -> str | int | None:
    if value is None:
        return None
    v = value.strip()
    if v.lower() == "none":
        return None
    if v.isdigit():
        return int(v)
    return v

# Skapar kommandoradsgränssnitt (CLI) med argparse
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Excel (cars) → SQLite")
    
    p.add_argument(
        "--excel",
        type=Path,
        default=Path("data/dataset_final.xlsx"),  # <-- Excel-filen i data/-mappen
        help="Sökväg till Excel-fil",
    )
    
    p.add_argument(
        "--db",
        type=Path,
        default=Path("dataset_final.db"),  # <-- Databasfilen som skapas/uppdateras
        help="SQLite-databasfil",
    )
    
    p.add_argument("--log", type=Path, default=Path("logs/app.log"), help="Loggfil")
    
    p.add_argument(
        "--sheet",
        type=_sheet_arg,
        default=None,
        help="Bladnamn eller index (valfritt)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # Visa vilka värden som faktiskt används
    print(
        f"Kör ETL med Excel={args.excel} → DB={args.db} (logg: {args.log}, sheet: {args.sheet})"
    )
    run_etl(args.excel, args.db, args.log, sheet=args.sheet)





