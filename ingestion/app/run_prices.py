import datetime as dt
from .config import TICKERS, HORIZON_DAYS, OUTPUT_URI, PRICES_PREFIX, MANIFEST_PREFIX
from .storage import Storage
from .validators import validate_prices
from .yahoo import download_prices

def _manifest_key(kind: str, ticker: str) -> str:
    return f"{MANIFEST_PREFIX.strip('/')}/{kind}/{ticker}.json"

def main():
    store = Storage(OUTPUT_URI)
    end = dt.date.today()
    start_default = end - dt.timedelta(days=HORIZON_DAYS)

    for t in TICKERS:
        manifest = store.read_json(_manifest_key("prices", t)) or {}
        start = dt.date.fromisoformat(manifest["last_date"]) + dt.timedelta(days=1) if "last_date" in manifest else start_default

        df = download_prices([t], start, end)
        df = validate_prices(df)

        if df.empty:
            continue

        load_date = end.isoformat()
        path = f"{PRICES_PREFIX}/{t}/{load_date}_prices.parquet"
        store.write_parquet(df, path)

        last_date = max(df["date"]).isoformat()
        store.write_json({"last_date": last_date}, _manifest_key("prices", t))

    return {"ok": True}

if __name__ == "__main__":
    print(main())
