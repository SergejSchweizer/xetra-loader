"""Start the REST application and its resident durable-job worker."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from wsgiref.simple_server import WSGIServer, make_server

from xetra_loader.api.worker import DurableJobWorker
from xetra_loader.config import resolve_api_token, resolve_cores, resolve_medallion_root
from xetra_loader.logging_config import configure_logging, shutdown_logging
from xetra_loader.sync.core import JSONValue

_LOGGER = logging.getLogger("xetra_loader.api")


class MonitoringWSGIServer(WSGIServer):
    """Stop the API process if its durable worker becomes unhealthy."""

    worker: DurableJobWorker | None = None

    def service_actions(self) -> None:
        if self.worker is not None and not self.worker.is_alive:
            raise RuntimeError("durable job worker is unavailable")


def _run_weekly_sync(job_id: str, cores: int) -> dict[str, JSONValue]:
    """Invoke the one canonical, restart-safe synchronization pipeline."""

    from xetra_loader.pipeline.restart import run_restartable_pipeline
    from xetra_loader.pipeline.runtime import build_weekly_stages

    root = Path(resolve_medallion_root()).resolve()
    _LOGGER.info("weekly_job_pipeline_start id=%s cores=%d", job_id, cores)
    summary = run_restartable_pipeline(
        build_weekly_stages(cores),
        lock_path=root / "weekly.lock",
        checkpoint_path=root / "weekly.checkpoint.parquet",
    )
    return {
        "status": "success" if summary.succeeded else "failed",
        "stages": [
            {"name": report.name, "status": report.status}
            for report in summary.reports
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="xdl-api")
    parser.add_argument("--host", default=os.getenv("XDL_API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("XDL_API_PORT", "8765")))
    parser.add_argument("--cores", type=int, default=None)
    arguments = parser.parse_args(argv)
    if not 1 <= arguments.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    cores = resolve_cores(arguments.cores)
    token = resolve_api_token()
    # Set this before importing any component that can initialize Polars.
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)
    log_path = configure_logging("xdl-api")

    from xetra_loader.api.application import create_application
    from xetra_loader.api.jobs import JobStore

    store = JobStore()
    worker = DurableJobWorker(store, cores=cores, job_runner=_run_weekly_sync)
    server: MonitoringWSGIServer | None = None
    try:
        worker.start()
        application = create_application(store=store, worker=worker, token=token)
        server = make_server(
            arguments.host,
            arguments.port,
            application,
            server_class=MonitoringWSGIServer,
        )
        server.worker = worker
        _LOGGER.info(
            "api_started host=%s port=%d cores=%d log=%s",
            arguments.host,
            arguments.port,
            cores,
            log_path,
        )
        server.serve_forever()
    except KeyboardInterrupt:
        _LOGGER.info("api_shutdown_requested")
    finally:
        if server is not None:
            server.server_close()
        worker.stop()
        shutdown_logging("xdl-api")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
