import io
import re

from setuptools import setup


def find_version():
    return re.search(
        r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        io.open("src/pyfdb/version.py", encoding="utf_8_sig").read(),
    ).group(1)


setup(version=find_version())
