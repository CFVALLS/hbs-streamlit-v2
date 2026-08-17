from pathlib import Path

from streamlit.testing.v1 import AppTest

from scripts.utils import helpers


class UnavailableResponse:
    status_code = 503
    text = "service unavailable"


def test_app_renders_without_database_or_online_api(monkeypatch):
    monkeypatch.setattr(
        helpers.requests,
        "post",
        lambda *args, **kwargs: UnavailableResponse(),
    )
    app_path = Path(__file__).resolve().parents[1] / "app.py"

    app = AppTest.from_file(str(app_path)).run(timeout=60)

    assert not app.exception
    download_buttons = app.get("download_button")
    assert len(download_buttons) == 2
    assert all(button.disabled for button in download_buttons)
