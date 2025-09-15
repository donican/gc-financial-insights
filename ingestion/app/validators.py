import pandas as pd

def validate_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    must = {"date","ticker","open","high","low","close","volume"}
    missing = must - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes em prices: {missing}")
    df = df.dropna(subset=["date","ticker","close"])
    df = df[df["close"] >= 0]
    df = df[df["volume"].fillna(0) >= 0]
    return df