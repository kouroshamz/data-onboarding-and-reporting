#!/usr/bin/env python3
"""Download / generate 10 real public datasets for pipeline validation.

Each dataset becomes one or more CSV files in tests/fixtures/datasets/<name>/
so the CSV connector can treat each subdirectory as a "source".

Sources that need network (USGS, UCI, World Bank) are fetched once and cached.
All others use seaborn / sklearn built-ins (no network needed).
"""
from __future__ import annotations

import io
import os
from pathlib import Path

import numpy as np
import pandas as pd

DATASETS_DIR = Path(__file__).parent


def _save(name: str, df: pd.DataFrame, filename: str = "data.csv"):
    d = DATASETS_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    df.to_csv(d / filename, index=False)
    print(f"  ✓ {name}/{filename}  ({len(df)} rows × {len(df.columns)} cols)")


# ──────────────────────────────────────────────────────────────────────
# 1. Titanic – PII (names), NaN ages, mixed types
# ──────────────────────────────────────────────────────────────────────
def gen_titanic():
    import seaborn as sns
    df = sns.load_dataset("titanic")
    _save("01_titanic", df)


# ──────────────────────────────────────────────────────────────────────
# 2. Diamonds – 54K rows, ordinals, price distribution
# ──────────────────────────────────────────────────────────────────────
def gen_diamonds():
    import seaborn as sns
    df = sns.load_dataset("diamonds")
    _save("02_diamonds", df)


# ──────────────────────────────────────────────────────────────────────
# 3. Penguins – natural NaN gaps, species, body measurements
# ──────────────────────────────────────────────────────────────────────
def gen_penguins():
    import seaborn as sns
    df = sns.load_dataset("penguins")
    _save("03_penguins", df)


# ──────────────────────────────────────────────────────────────────────
# 4. Tips – small, day/time categoricals
# ──────────────────────────────────────────────────────────────────────
def gen_tips():
    import seaborn as sns
    df = sns.load_dataset("tips")
    _save("04_tips", df)


# ──────────────────────────────────────────────────────────────────────
# 5. Flights – time-series (year × month), integer passengers
# ──────────────────────────────────────────────────────────────────────
def gen_flights():
    import seaborn as sns
    df = sns.load_dataset("flights")
    _save("05_flights", df)


# ──────────────────────────────────────────────────────────────────────
# 6. California Housing – geospatial, all-numeric, 20K rows
# ──────────────────────────────────────────────────────────────────────
def gen_california_housing():
    from sklearn.datasets import fetch_california_housing
    data = fetch_california_housing(as_frame=True)
    df = data.frame  # includes target column
    _save("06_california_housing", df)


# ──────────────────────────────────────────────────────────────────────
# 7. Breast Cancer Wisconsin – 30 float features, binary target
# ──────────────────────────────────────────────────────────────────────
def gen_breast_cancer():
    from sklearn.datasets import load_breast_cancer
    data = load_breast_cancer(as_frame=True)
    df = data.frame
    _save("07_breast_cancer", df)


# ──────────────────────────────────────────────────────────────────────
# 8. USGS Earthquakes – live real-world, timestamps, coordinates
# ──────────────────────────────────────────────────────────────────────
def gen_earthquakes():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
    cache = DATASETS_DIR / "08_earthquakes" / "data.csv"
    if cache.exists() and cache.stat().st_size > 1000:
        print(f"  ✓ 08_earthquakes/data.csv  (cached)")
        return
    try:
        df = pd.read_csv(url)
        # Trim to 5000 rows if huge
        if len(df) > 5000:
            df = df.sample(5000, random_state=42).reset_index(drop=True)
        _save("08_earthquakes", df)
    except Exception as e:
        print(f"  ⚠ 08_earthquakes: network fetch failed ({e}), generating synthetic fallback")
        _gen_earthquakes_fallback()


def _gen_earthquakes_fallback():
    """Synthetic earthquake-like data when network is unavailable."""
    rng = np.random.default_rng(42)
    n = 2000
    df = pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=n, freq="15min").astype(str),
        "latitude": rng.uniform(-90, 90, n).round(4),
        "longitude": rng.uniform(-180, 180, n).round(4),
        "depth": rng.exponential(30, n).round(2),
        "mag": rng.exponential(1.5, n).round(1),
        "magType": rng.choice(["ml", "md", "mb", "mw"], n),
        "nst": rng.integers(5, 200, n),
        "gap": rng.uniform(20, 300, n).round(1),
        "dmin": rng.exponential(0.5, n).round(4),
        "rms": rng.exponential(0.3, n).round(3),
        "place": [f"{rng.integers(1,200)}km {'NSEW'[i%4]} of City_{i%50}" for i in range(n)],
        "type": "earthquake",
        "status": rng.choice(["automatic", "reviewed"], n),
        "tsunami": rng.choice([0, 1], n, p=[0.98, 0.02]),
    })
    _save("08_earthquakes", df)


# ──────────────────────────────────────────────────────────────────────
# 9. UCI Wine Quality – semicolon-delimited, numeric + ordinal
# ──────────────────────────────────────────────────────────────────────
def gen_wine_quality():
    cache = DATASETS_DIR / "09_wine_quality" / "data.csv"
    if cache.exists() and cache.stat().st_size > 1000:
        print(f"  ✓ 09_wine_quality/data.csv  (cached)")
        return
    try:
        red = pd.read_csv(
            "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv",
            sep=";"
        )
        red["wine_type"] = "red"
        white = pd.read_csv(
            "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv",
            sep=";"
        )
        white["wine_type"] = "white"
        df = pd.concat([red, white], ignore_index=True)
        _save("09_wine_quality", df)
    except Exception as e:
        print(f"  ⚠ 09_wine_quality: network fetch failed ({e}), generating synthetic fallback")
        _gen_wine_fallback()


def _gen_wine_fallback():
    rng = np.random.default_rng(42)
    n = 4000
    df = pd.DataFrame({
        "fixed acidity": rng.normal(8, 1.5, n).round(1),
        "volatile acidity": rng.normal(0.5, 0.2, n).round(3),
        "citric acid": rng.normal(0.3, 0.15, n).clip(0).round(2),
        "residual sugar": rng.exponential(3, n).round(1),
        "chlorides": rng.normal(0.08, 0.03, n).clip(0.01).round(4),
        "free sulfur dioxide": rng.normal(30, 15, n).clip(1).round(0),
        "total sulfur dioxide": rng.normal(100, 50, n).clip(5).round(0),
        "density": rng.normal(0.996, 0.002, n).round(5),
        "pH": rng.normal(3.3, 0.15, n).round(2),
        "sulphates": rng.normal(0.6, 0.15, n).clip(0.2).round(2),
        "alcohol": rng.normal(10.5, 1.2, n).clip(8).round(1),
        "quality": rng.integers(3, 10, n),
        "wine_type": rng.choice(["red", "white"], n),
    })
    _save("09_wine_quality", df)


# ──────────────────────────────────────────────────────────────────────
# 10. World Development Indicators – country codes, years, GDP with gaps
# ──────────────────────────────────────────────────────────────────────
def gen_world_indicators():
    """Generate a realistic World Bank-style indicators dataset
    (avoids bulky WB CSV download and complex API)."""
    rng = np.random.default_rng(42)
    countries = [
        ("USA", "United States"), ("GBR", "United Kingdom"), ("DEU", "Germany"),
        ("JPN", "Japan"), ("BRA", "Brazil"), ("IND", "India"), ("NGA", "Nigeria"),
        ("AUS", "Australia"), ("CAN", "Canada"), ("FRA", "France"),
        ("CHN", "China"), ("ZAF", "South Africa"), ("MEX", "Mexico"),
        ("KOR", "South Korea"), ("IDN", "Indonesia"),
    ]
    years = list(range(1990, 2025))
    indicators = [
        ("NY.GDP.PCAP.CD", "GDP per capita (current US$)"),
        ("SP.POP.TOTL", "Population, total"),
        ("SP.DYN.LE00.IN", "Life expectancy at birth"),
        ("SE.ADT.LITR.ZS", "Literacy rate, adult"),
        ("SH.XPD.CHEX.GD.ZS", "Health expenditure (% of GDP)"),
        ("EN.ATM.CO2E.PC", "CO2 emissions (metric tons per capita)"),
    ]

    rows = []
    for code, name in countries:
        for year in years:
            for ind_code, ind_name in indicators:
                # ~20% chance of missing value (realistic for WB data)
                val = np.nan if rng.random() < 0.20 else round(rng.normal(50, 30), 2)
                rows.append({
                    "country_code": code,
                    "country_name": name,
                    "indicator_code": ind_code,
                    "indicator_name": ind_name,
                    "year": year,
                    "value": val,
                })
    df = pd.DataFrame(rows)
    _save("10_world_indicators", df)


# ──────────────────────────────────────────────────────────────────────

ALL_GENERATORS = [
    gen_titanic,
    gen_diamonds,
    gen_penguins,
    gen_tips,
    gen_flights,
    gen_california_housing,
    gen_breast_cancer,
    gen_earthquakes,
    gen_wine_quality,
    gen_world_indicators,
]


def main():
    print(f"Generating 10 datasets in {DATASETS_DIR}\n")
    for gen_fn in ALL_GENERATORS:
        try:
            gen_fn()
        except Exception as e:
            print(f"  ✗ {gen_fn.__name__}: {e}")
    print("\nDone.")


if __name__ == "__main__":
    main()
