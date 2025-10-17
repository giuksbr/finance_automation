import pandas as pd
from src.indicators import rsi, atr, bollinger_bands

def test_indicators_basic():
    closes = pd.Series([i for i in range(1, 50)])
    r = rsi(closes, 14)
    assert len(r) == 49
    ma, up, lo = bollinger_bands(closes, 20, 2.0)
    assert ma.iloc[-1] is not None
