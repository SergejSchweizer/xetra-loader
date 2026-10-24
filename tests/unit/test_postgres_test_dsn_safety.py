import pytest

from xetra_loader.testing import validate_local_test_postgres_dsn


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://tester:secret@localhost:5432/xetra_loader_test",
        "dbname=test_xetra host=127.0.0.1 user=tester",
        "dbname=xetra_test hostaddr=::1",
        "dbname=xetra_test",
    ],
)
def test_explicit_local_test_databases_are_allowed(dsn: str) -> None:
    validate_local_test_postgres_dsn(dsn)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://tester:secret@localhost:5432/xetra_loader",
        "dbname=production_test host=db.example.com",
        "dbname=postgres host=127.0.0.1",
        "host=localhost",
    ],
)
def test_ambiguous_or_remote_database_targets_are_rejected(dsn: str) -> None:
    with pytest.raises(ValueError, match="XDL_TEST_POSTGRES_DSN"):
        validate_local_test_postgres_dsn(dsn)
