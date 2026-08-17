from unittest.mock import MagicMock
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models_orm import (
    Base,
    CentralCostoOperacional,
    CentralTable,
    DesacopleHistory,
    StatusCentral,
)
from db.operaciones_db import (
    get_cmg_programados,
    get_status_central_history,
    query_central_table_modifications,
    query_cmg_ponderado_by_time,
    retrieve_status_desacople,
)


def test_empty_weighted_cmg_query_returns_no_synthetic_rows():
    session = MagicMock()
    session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

    assert query_cmg_ponderado_by_time(session, 1_700_000_000, 48) == []


def test_empty_programmed_cmg_query_returns_empty_mapping():
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    assert get_cmg_programados(session, "Quillota", "2026-08-17") == {}


def test_weighted_cmg_query_propagates_database_failure():
    session = MagicMock()
    session.query.side_effect = RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        query_cmg_ponderado_by_time(session, 1_700_000_000, 48)


def create_central_tables():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            CentralTable.__table__,
            CentralCostoOperacional.__table__,
            StatusCentral.__table__,
        ],
    )
    return engine


def test_central_history_uses_cost_linked_to_each_revision():
    engine = create_central_tables()
    with Session(engine) as session:
        first = CentralTable(
            nombre="Quillota",
            external_update=True,
            fecha_registro="1970-01-01 00:01:40",
        )
        second = CentralTable(
            nombre="Quillota",
            external_update=True,
            fecha_registro="1970-01-01 00:03:20",
        )
        session.add_all([first, second])
        session.flush()
        session.add_all(
            [
                CentralCostoOperacional(
                    central_id=first.id,
                    central_nombre="Quillota",
                    unix_time=150,
                    costo_operacional=10,
                ),
                CentralCostoOperacional(
                    # Legacy data can point to an older revision even though its
                    # timestamp belongs to the newer revision interval.
                    central_id=first.id,
                    central_nombre="Quillota",
                    unix_time=250,
                    costo_operacional=20,
                ),
            ]
        )
        session.commit()

        result = query_central_table_modifications(session, num_entries=10)

    assert result["costo_operacional"].astype(float).tolist() == [20, 10]


def test_status_history_returns_full_window_and_predecessor():
    engine = create_central_tables()
    with Session(engine) as session:
        central = CentralTable(nombre="Los Angeles")
        session.add(central)
        session.flush()
        cost = CentralCostoOperacional(
            central_id=central.id,
            central_nombre="Los Angeles",
            unix_time=900,
            costo_operacional=45,
        )
        session.add(cost)
        session.flush()
        session.add(
            StatusCentral(
                central="Los Angeles",
                barra="CHARRUA__220",
                timestamp="1970-01-01 00:16:39",
                unix_time=999,
                cmg_ponderado=44,
                status_operacional="OFF",
                costo_operacional_id=cost.id,
            )
        )
        for unix_time in range(1000, 1060):
            session.add(
                StatusCentral(
                    central="Los Angeles",
                    barra="CHARRUA__220",
                    timestamp="1970-01-01 00:16:40",
                    unix_time=unix_time,
                    cmg_ponderado=46,
                    status_operacional="ON" if unix_time % 2 else "OFF",
                    costo_operacional_id=cost.id,
                )
            )
        session.commit()

        result = get_status_central_history(
            session,
            limit=50,
            centrals=["Los Angeles"],
            since_unix=1000,
        )

    assert len(result) == 61
    assert result["unix_time"].min() == 999


def test_missing_desacople_history_returns_unknown_state():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[DesacopleHistory.__table__])

    with Session(engine) as session:
        assert retrieve_status_desacople(session, "CHARRUA__220") == (None, None)


def test_explicit_acople_and_desacople_events_are_distinguished():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[DesacopleHistory.__table__])

    with Session(engine) as session:
        session.add_all(
            [
                DesacopleHistory(
                    barra_transmision="CHARRUA__220",
                    estado="desacople",
                    detected_at=datetime(2026, 8, 17, 10),
                ),
                DesacopleHistory(
                    barra_transmision="CHARRUA__220",
                    estado="acople",
                    detected_at=datetime(2026, 8, 17, 11),
                ),
            ]
        )
        session.commit()

        assert retrieve_status_desacople(session, "CHARRUA__220") == (False, None)


def test_missing_desacople_table_is_reported_as_schema_error():
    engine = create_engine("sqlite:///:memory:")

    with Session(engine) as session:
        with pytest.raises(RuntimeError, match="desacople_history"):
            retrieve_status_desacople(session, "CHARRUA__220")
