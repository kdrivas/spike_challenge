from setuptools import setup

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(name="dags", install_requires=install_requires,)