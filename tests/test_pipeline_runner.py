from pathlib import Path

from etl.run_pipeline import build_pipeline_steps


def test_build_pipeline_steps_returns_expected_scripts():
    project_root = Path(__file__).resolve().parents[1]

    steps = build_pipeline_steps(project_root)

    expected = [
        project_root / "etl" / "bronze" / "load_bronze_cash.py",
        project_root / "etl" / "silver" / "load_silver_cash.py",
        project_root / "etl" / "bronze" / "load_bronze_award.py",
        project_root / "etl" / "gold" / "build_gold_analytics.py",
        project_root / "etl" / "quality" / "validate_gold.py",
        project_root / "search" / "index_gold_flights.py",
    ]

    assert [step["script"] for step in steps] == [str(path) for path in expected]
