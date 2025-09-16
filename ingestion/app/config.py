import os

TICKERS = [t.strip() for t in os.getenv("TICKERS", "AAPL,MSFT,AMZN,GOOGL,META").split(",") if t.strip()]
HORIZON_DAYS = int(os.getenv("HORIZON_DAYS", "30"))

OUTPUT_URI = os.getenv("OUTPUT_URI", "gs://gc-financial-insights-dev-bucket")
PRICES_PREFIX = os.getenv("PRICES_PREFIX", "bronze/prices")

MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "_manifests")
