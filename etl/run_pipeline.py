import subprocess
import sys
from pathlib import Path


def build_pipeline_steps():
    project_root = Path(__file__).resolve().parents[1].resolve()
    return [
        {"name": "bronze-cash", "script": str(project_root / "etl" / "bronze" / "load_bronze_cash.py")},
        {"name": "silver-cash", "script": str(project_root / "etl" / "silver" / "load_silver_cash.py")},
        {"name": "bronze-award", "script": str(project_root / "etl" / "bronze" / "load_bronze_award.py")},
        {"name": "gold-analytics", "script": str(project_root / "etl" / "gold" / "build_gold_analytics.py")},
    ]


def run_pipeline():
    project_root = Path(__file__).resolve().parents[1].resolve()
    python_executable = sys.executable
    results = []

    for step in build_pipeline_steps():
        completed = subprocess.run(
            [python_executable, step["script"]],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )

        results.append(
            {
                "name": step["name"],
                "script": step["script"],
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
        )

        if completed.returncode != 0:
            break

    return results


if __name__ == "__main__":
    results = run_pipeline()
    
    for result in results:
        print(f"[{result['name']}] exit={result['returncode']}")
        if result.get("stdout"):
            print(result["stdout"])
        if result.get("stderr"):
            print(result["stderr"])
