import pytest
from sqlalchemy import create_engine

from db import connection_db


def test_mysql_engine_uses_connection_liveness_options(monkeypatch):
    captured = {}
    sentinel = object()

    def fake_create_engine(url, **options):
        captured["url"] = url
        captured["options"] = options
        return sentinel

    monkeypatch.setattr(connection_db, "create_engine", fake_create_engine)

    engine = connection_db.establecer_engine(
        "mysql+pymysql://user:password@example.test:3306/database"
    )

    assert engine is sentinel
    assert captured["options"]["pool_pre_ping"] is True
    assert captured["options"]["pool_recycle"] == 300
    assert captured["options"]["pool_timeout"] == 10
    assert captured["options"]["connect_args"]["connect_timeout"] == 10


def test_connection_health_check_executes_select():
    engine = create_engine("sqlite:///:memory:")

    assert connection_db.verificar_conexion(engine) is True


def test_unsupported_mysql_driver_is_rejected():
    with pytest.raises(ValueError, match="Driver MySQL no soportado"):
        connection_db.establecer_engine(
            "mysql+mysqlconnector://user:password@example.test:3306/database"
        )


def test_session_scope_rolls_back_and_closes_on_error():
    class FakeSession:
        def __init__(self):
            self.rolled_back = False
            self.closed = False

        def commit(self):
            raise AssertionError("commit should not run")

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

    session = FakeSession()

    with pytest.raises(RuntimeError, match="query failed"):
        with connection_db.session_scope(session):
            raise RuntimeError("query failed")

    assert session.rolled_back is True
    assert session.closed is True
