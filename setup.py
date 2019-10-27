from setuptools import setup

setup(
    name="postgres-create-tables",
    version="0.1",
    description="Tool for automating table creation in postgres",
    license="MIT",
    packages=["create_tables_tool"],
    install_requires=["psycopg2==2.7.7", "psycopg2-binary==2.8.4"]
)