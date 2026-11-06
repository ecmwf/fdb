import subprocess
from pathlib import Path
from typing import Optional


def run_script(script: Path, args: Optional[list[str]], cwd: str, env: dict[str, str]):
    command = [str(script)]

    if args is not None:
        command.extend(args)

    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise AssertionError(
            f"{script.name} failed with exit code {result.returncode}:\n{result.stdout}"
        )

    return result.stdout
