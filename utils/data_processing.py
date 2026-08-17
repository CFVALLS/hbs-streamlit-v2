"""Pure dataframe transformations shared by the dashboard and downloads."""

import pandas as pd


BAR_ALIASES = {
    "CHARRUA": "CHARRUA__220",
    "CHARRUA_220": "CHARRUA__220",
    "CHARRUA_22O": "CHARRUA__220",
    "CHARRUA__220": "CHARRUA__220",
    "QUILLOTA": "QUILLOTA__220",
    "QUILLOTA_220": "QUILLOTA__220",
    "QUILLOTA_22O": "QUILLOTA__220",
    "QUILLOTA__220": "QUILLOTA__220",
}

CENTRAL_BY_BAR = {
    "CHARRUA__220": "Los Angeles",
    "QUILLOTA__220": "Quillota",
}


def normalize_barra(value):
    """Return the canonical identifier for known transmission bar variants."""
    if pd.isna(value):
        return value
    normalized = str(value).strip().upper()
    return BAR_ALIASES.get(normalized, normalized)


def parse_mixed_timestamps(values: pd.Series) -> pd.Series:
    """Parse supported timestamp formats without destroying unparsed source values."""
    source = values.astype("string").str.strip()

    def parse_one(value):
        if pd.isna(value):
            return pd.NaT

        parsed = pd.NaT
        for date_format in (
            "%d.%m.%y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            parsed = pd.to_datetime(value, format=date_format, errors="coerce")
            if not pd.isna(parsed):
                break
        if pd.isna(parsed):
            parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return pd.NaT
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert("America/Santiago").tz_localize(None)
        return parsed

    return pd.to_datetime(source.map(parse_one), errors="coerce")


def normalize_cmg_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize CMg rows and discard records that cannot be plotted truthfully."""
    columns = [
        "barra_transmision",
        "timestamp",
        "unix_time",
        "cmg_ponderado",
        "fecha",
        "hora",
        "central",
    ]
    if dataframe is None or dataframe.empty:
        return pd.DataFrame(columns=columns)

    normalized = dataframe.copy()
    required = {"barra_transmision", "timestamp", "cmg_ponderado"}
    if not required.issubset(normalized.columns):
        return pd.DataFrame(columns=columns)

    normalized["barra_transmision"] = normalized["barra_transmision"].map(normalize_barra)
    normalized["timestamp"] = parse_mixed_timestamps(normalized["timestamp"])
    normalized["cmg_ponderado"] = pd.to_numeric(
        normalized["cmg_ponderado"], errors="coerce"
    )
    normalized = normalized.dropna(
        subset=["barra_transmision", "timestamp", "cmg_ponderado"]
    )
    normalized["fecha"] = normalized["timestamp"].dt.strftime("%Y-%m-%d")
    normalized["hora"] = normalized["timestamp"].dt.strftime("%H:%M:%S")
    normalized["central"] = normalized["barra_transmision"].map(CENTRAL_BY_BAR)
    return normalized.sort_values("timestamp").reset_index(drop=True)


def filter_by_bar(dataframe: pd.DataFrame, barra: str) -> pd.DataFrame:
    """Filter a dataframe by a canonical bar identifier."""
    if dataframe is None or dataframe.empty or "barra_transmision" not in dataframe.columns:
        return pd.DataFrame(columns=dataframe.columns if dataframe is not None else None)

    canonical_bar = normalize_barra(barra)
    normalized_bars = dataframe["barra_transmision"].map(normalize_barra)
    return dataframe.loc[normalized_bars == canonical_bar].copy()


def prepare_download_dataframe(
    dataframe: pd.DataFrame,
    barra: str,
    minimum_unix_time: int,
) -> pd.DataFrame:
    """Apply the exact download lower bound and selected-bar filter."""
    filtered = filter_by_bar(dataframe, barra)
    if filtered.empty or "unix_time" not in filtered.columns:
        return filtered.iloc[0:0].copy()

    unix_times = pd.to_numeric(filtered["unix_time"], errors="coerce")
    return filtered.loc[unix_times >= minimum_unix_time].copy()


def stale_cmg_for_range(cache, time_range_hours):
    """Return stale CMg only when it was fetched for the requested range."""
    cached = (cache or {}).get(time_range_hours)
    if not cached or not isinstance(cached.get("data"), pd.DataFrame):
        return normalize_cmg_dataframe(pd.DataFrame()), None
    return cached["data"].copy(), cached.get("success")
