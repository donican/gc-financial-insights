import datetime as dt
import time
import pandas as pd
import yfinance as yf

def _today():
    return dt.date.today()

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def download_prices(tickers, start, end, pause=0.8, chunk_size=5) -> pd.DataFrame:
    """Baixa OHLCV ajustado, normaliza para (date, ticker, open, high, low, close, volume)."""
    frames = []
    for batch in chunked(tickers, chunk_size):
        data = yf.download(batch, start=start, end=end, auto_adjust=True, group_by="ticker", progress=False)
        for t in batch:
            try:
                sub = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                sub = sub.rename(columns=str.lower).reset_index().rename(columns={"Date": "date"})
                sub["ticker"] = t
                frames.append(sub[["date","ticker","open","high","low","close","volume"]])
            except Exception:
                pass
        time.sleep(pause)
    if not frames:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out