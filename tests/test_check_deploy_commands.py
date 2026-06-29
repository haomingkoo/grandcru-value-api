from pathlib import Path

from scripts.check_deploy_commands import command_text, command_tokens


def test_command_text_strips_comments_and_line_continuations(tmp_path: Path) -> None:
    path = tmp_path / "daily-ingest-command.txt"
    path.write_text(
        """
        # ignored
        python scripts/refresh_pipeline.py \\
          --scrape-and-build \\
          --health-url https://example.com/health
        """,
        encoding="utf-8",
    )

    assert command_text(path) == (
        "python scripts/refresh_pipeline.py --scrape-and-build "
        "--health-url https://example.com/health"
    )


def test_command_tokens_normalizes_spacing() -> None:
    assert command_tokens("python  script.py   --flag value") == [
        "python",
        "script.py",
        "--flag",
        "value",
    ]
