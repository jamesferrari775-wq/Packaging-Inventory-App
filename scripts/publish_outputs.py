from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True)


def has_changes(workspace: Path) -> bool:
    status = run(["git", "status", "--porcelain", "--", "outputs"], workspace)
    return status.returncode == 0 and bool(status.stdout.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Commit/push latest generated output files")
    parser.add_argument("--push", action="store_true", help="Push commit after creating it")
    parser.add_argument("--remote", default="origin", help="Git remote name")
    parser.add_argument("--branch", default="main", help="Git branch to push")
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parent.parent

    add = run(["git", "add", "outputs"], workspace)
    if add.returncode != 0:
        print(add.stderr.strip() or "git add failed")
        raise SystemExit(add.returncode)

    if not has_changes(workspace):
        print("No output changes to publish.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    message = f"chore(outputs): update generated files ({stamp})"

    commit = run(["git", "commit", "-m", message], workspace)
    if commit.returncode != 0:
        print(commit.stdout.strip())
        print(commit.stderr.strip())
        raise SystemExit(commit.returncode)

    print(commit.stdout.strip())

    if args.push:
        push = run(["git", "push", args.remote, args.branch], workspace)
        print(push.stdout.strip())
        if push.returncode != 0:
            print(push.stderr.strip())
            raise SystemExit(push.returncode)


if __name__ == "__main__":
    main()
