from setuptools import setup

setup(
    name="postgres-create-tables",
    version="0.1",
    description="Tool for automating table creation in postgres",
    license="MIT",
    install_requires=["mysql-connector-python==8.0.18","fire"]
)

fire==0.2.1
