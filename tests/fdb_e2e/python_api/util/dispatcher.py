import logging
import os
import shlex
import subprocess
from pathlib import Path


def run_request(cmd: str, workdir: Path, env) -> int:
    env = {**os.environ.copy(), **env}
    lex = shlex.shlex(cmd)
    lex.whitespace_split = True

    lexed_command = list(lex)

    log_file_name = workdir / "test_log.txt"

    with subprocess.Popen(
        lexed_command,
        env=env,
        cwd=workdir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ) as process:
        process.stdin.flush()
        process.stdin.close()

        while True:
            line = process.stdout.readline()
            if not line:
                break
            logging.debug(line.decode("utf-8").rstrip())
            with log_file_name.open("a+") as log_file:
                log_file.write(line.decode("utf-8"))

    return process.returncode
