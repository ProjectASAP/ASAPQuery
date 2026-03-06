from setuptools import setup, find_packages

setup(
    name="promql_utilities",
    version="0.1",
    packages=find_packages(),
    install_requires=["promql-parser>=0.4.1", "pandas", "pyarrow"],
)
