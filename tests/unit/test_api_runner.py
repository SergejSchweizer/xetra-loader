from __future__ import annotations

import signal
import threading
from types import SimpleNamespace

import xetra_loader.api.application as api_application
import xetra_loader.api.jobs as api_jobs
import xetra_loader.api.runner as api_runner
from xetra_loader.api.runner import _install_sigterm_shutdown


class _Server:
    def __init__(self) -> None:
        self.shutdown_called = threading.Event()

    def shutdown(self) -> None:
        self.shutdown_called.set()


def test_sigterm_handler_routes_systemd_stop_through_server_shutdown(monkeypatch) -> None:
    server = _Server()
    installed: list[tuple[int, object]] = []

    def fake_signal(signum: int, handler: object) -> object:
        installed.append((signum, handler))
        return signal.SIG_DFL

    monkeypatch.setattr(signal, "signal", fake_signal)

    previous = _install_sigterm_shutdown(server)  # type: ignore[arg-type]
    signum, handler = installed[0]
    assert signum == signal.SIGTERM
    assert previous == signal.SIG_DFL
    assert callable(handler)

    handler(signal.SIGTERM, None)  # type: ignore[operator]

    assert server.shutdown_called.wait(timeout=1)


def test_api_main_stops_worker_on_systemd_sigterm(monkeypatch) -> None:
    shutdown = threading.Event()
    worker_stopped = threading.Event()
    signal_handlers: dict[int, object] = {}

    class _Worker:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def start(self) -> None:
            return None

        def stop(self) -> None:
            worker_stopped.set()

    class _Server:
        worker = None

        def serve_forever(self) -> None:
            handler = signal_handlers[signal.SIGTERM]
            handler(signal.SIGTERM, None)  # type: ignore[operator]
            assert shutdown.wait(timeout=1)

        def shutdown(self) -> None:
            shutdown.set()

        def server_close(self) -> None:
            return None

    def fake_signal(signum: int, handler: object) -> object:
        previous = signal_handlers.get(signum, signal.SIG_DFL)
        signal_handlers[signum] = handler
        return previous

    monkeypatch.setattr(api_runner, "resolve_api_token", lambda: "unit-token")
    monkeypatch.setattr(api_runner, "resolve_cores", lambda _cores: 2)
    monkeypatch.setattr(api_runner, "configure_logging", lambda _name: None)
    monkeypatch.setattr(api_runner, "shutdown_logging", lambda _name: None)
    monkeypatch.setattr(api_runner, "DurableJobWorker", _Worker)
    monkeypatch.setattr(api_jobs, "JobStore", lambda: SimpleNamespace())
    monkeypatch.setattr(api_application, "create_application", lambda **_kwargs: object())
    monkeypatch.setattr(api_runner, "make_server", lambda *_args, **_kwargs: _Server())
    monkeypatch.setattr(signal, "signal", fake_signal)

    assert api_runner.main(["--cores", "2"]) == 0
    assert worker_stopped.is_set()
