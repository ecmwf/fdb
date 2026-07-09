import logging
import subprocess
from pathlib import Path
from typing import Optional


def run_script(script: Path, args: Optional[list[str]], cwd: str, env: dict[str, str]):
    command = [str(script)]

    if args is not None:
        command.extend(args)

    logging.debug(f"Calling {command}")

    logging.debug(env)

    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )

    stdout = ""

    for line in process.stdout:
        if not line:
            break
        logging.info(line.strip())
        stdout += line

    process.wait()

    if process.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=process.returncode, cmd=command, output=stdout
        )

    return stdout
