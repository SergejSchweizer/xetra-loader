from pathlib import Path


def test_github_actions_workflows_are_retired_by_repository_policy() -> None:
    workflows = Path(".github/workflows")
    assert not workflows.exists() or not tuple(workflows.glob("*.yml"))
    assert not workflows.exists() or not tuple(workflows.glob("*.yaml"))
