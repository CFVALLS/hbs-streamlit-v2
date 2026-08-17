import json
import time

import pandas as pd
import pytest

from db import operaciones_db
from scripts.utils import helpers


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.text = json.dumps(payload)


def test_online_cmg_accepts_wrapped_payload_and_accented_bar(monkeypatch):
    response = FakeResponse(
        {
            "data": [
                {"barra": "Charrúa", "fecha": "2026-08-17 10:00:00", "cmg": "51.2"},
                {"barra": "Quillota", "fecha": "2026-08-17 10:00:00", "cmg": 49.1},
            ]
        }
    )
    monkeypatch.setattr(helpers.requests, "post", lambda *args, **kwargs: response)

    result = helpers.get_costo_marginal_online_hora(
        "2026-08-17",
        "2026-08-17",
        ["Charrua", "Quillota"],
        "10:00:00",
        user_key="test",
    )

    assert result == {"Charrua": 51.2, "Quillota": 49.1}


def test_online_cmg_allows_partial_response(monkeypatch):
    response = FakeResponse(
        [{"barra": "Quillota", "fecha": "2026-08-17 10:15:00", "cmg": 49.1}]
    )
    monkeypatch.setattr(helpers.requests, "post", lambda *args, **kwargs: response)

    result = helpers.get_costo_marginal_online_hora(
        "2026-08-17",
        "2026-08-17",
        ["Charrua", "Quillota"],
        "10:00:00",
    )

    assert result == {"Quillota": 49.1}


def test_online_cmg_uses_latest_reading_regardless_of_payload_order(monkeypatch):
    response = FakeResponse(
        [
            {"barra": "Quillota", "fecha": "2026-08-17 10:45:00", "cmg": 55},
            {"barra": "Quillota", "fecha": "2026-08-17 10:05:00", "cmg": 40},
        ]
    )
    monkeypatch.setattr(helpers.requests, "post", lambda *args, **kwargs: response)

    result = helpers.get_costo_marginal_online_hora(
        "2026-08-17",
        "2026-08-17",
        ["Quillota"],
        "10:00:00",
    )

    assert result == {"Quillota": 55}


def test_status_piechart_handles_single_status_row():
    now = int(time.time())
    dataframe = pd.DataFrame(
        {"unix_time": [now - 1800], "status_operacional": ["ON"]}
    )

    figure = helpers.create_status_piechart(dataframe, "Los Angeles", time_range=12)

    assert len(figure.data) == 1
    assert figure.data[0].labels.tolist() == ["ON"]
    assert figure.data[0].values[0] > 0


def test_programmed_cmg_database_failure_propagates(monkeypatch):
    monkeypatch.setattr(
        operaciones_db,
        "get_cmg_programados",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("database unavailable")),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        helpers.get_cmg_programados(
            "Quillota",
            "2026-08-17",
            session=object(),
        )
