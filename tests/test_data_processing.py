import pandas as pd

from utils.data_processing import (
    filter_by_bar,
    normalize_cmg_dataframe,
    prepare_download_dataframe,
    stale_cmg_for_range,
)


def test_normalize_cmg_dataframe_preserves_supported_mixed_timestamps():
    source = pd.DataFrame(
        {
            "barra_transmision": ["charrua_22o", "QUILLOTA_220", "CHARRUA"],
            "timestamp": [
                "17.08.26 10:00:00",
                "2026-08-17 11:00:00",
                "not-a-date",
            ],
            "unix_time": [1, 2, 3],
            "cmg_ponderado": ["45.25", 48.75, 99],
        }
    )

    result = normalize_cmg_dataframe(source)

    assert len(result) == 2
    assert result["barra_transmision"].tolist() == ["CHARRUA__220", "QUILLOTA__220"]
    assert result["central"].tolist() == ["Los Angeles", "Quillota"]
    assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
    assert result["cmg_ponderado"].tolist() == [45.25, 48.75]


def test_filter_by_bar_accepts_historical_aliases():
    source = pd.DataFrame(
        {
            "barra_transmision": ["CHARRUA__220", "quillota_22o"],
            "value": [1, 2],
        }
    )

    result = filter_by_bar(source, "QUILLOTA__220")

    assert result["value"].tolist() == [2]


def test_normalize_empty_dataframe_has_rendering_schema():
    result = normalize_cmg_dataframe(pd.DataFrame())

    assert {
        "barra_transmision",
        "timestamp",
        "cmg_ponderado",
        "central",
    }.issubset(result.columns)


def test_normalize_cmg_dataframe_converts_aware_timestamp_to_chile_local_time():
    source = pd.DataFrame(
        {
            "barra_transmision": ["CHARRUA__220", "QUILLOTA__220"],
            "timestamp": ["2026-08-17T14:00:00Z", "2026-08-17 10:00:00"],
            "cmg_ponderado": [45, 48],
        }
    )

    result = normalize_cmg_dataframe(source)

    assert result["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist() == [
        "2026-08-17 10:00:00",
        "2026-08-17 10:00:00",
    ]


def test_prepare_download_dataframe_applies_exact_bound_and_bar():
    source = pd.DataFrame(
        {
            "barra_transmision": [
                "CHARRUA__220",
                "CHARRUA__220",
                "QUILLOTA__220",
            ],
            "unix_time": [999, 1000, 1001],
            "value": [1, 2, 3],
        }
    )

    result = prepare_download_dataframe(source, "charrua_22o", 1000)

    assert result["value"].tolist() == [2]
    assert result.to_csv(index=False).startswith("barra_transmision,unix_time,value")


def test_stale_cmg_is_not_reused_for_another_range():
    cached_data = normalize_cmg_dataframe(
        pd.DataFrame(
            {
                "barra_transmision": ["CHARRUA__220"],
                "timestamp": ["2026-08-17 10:00:00"],
                "cmg_ponderado": [45],
            }
        )
    )
    cache = {168: {"data": cached_data, "success": "today"}}

    result, success = stale_cmg_for_range(cache, 12)

    assert result.empty
    assert success is None
