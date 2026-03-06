import logging
import subprocess
from pathlib import Path
from typing import Optional

# def run_script(script: Path, cwd, env):
#     result = subprocess.run([str(script)], cwd=cwd, env=env, capture_output=True, text=True)
#     if result.returncode != 0:
#         raise AssertionError(f"Shell script failed: {script.name}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")


def run_script(script: Path, args: Optional[list[str]], cwd: str, env: dict[str, str]):
    command = [str(script)]
    if args is not None:
        logging.debug("Appending")
        if len(args) > 1:
            command.extend(args)
        else:
            command.append(args[0])

    logging.debug(f"Calling {command}")

    logging.debug(env)

    process = subprocess.Popen(
        command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, text=True
    )

    stdout = ""

    for line in process.stdout:
        if not line:
            break
        logging.info(line.strip())
        stdout += line

    process.wait()

    if process.returncode != 0:
        raise subprocess.CalledProcessError(returncode=process.returncode, cmd=str(script))

    return stdout
