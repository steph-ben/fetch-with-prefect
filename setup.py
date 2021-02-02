from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="fetch-with-prefect",
    version="0.0.1",
    author="steph-ben",
    author_email = "stephane.benchimol@gmail.com",
    description="An demonstration of how to fetch data using prefect",
    url="https://github.com/steph-ben/fetch-with-prefect",
    packages=[
        'fetchers',
        'fetchers.s3',
    ],
    install_requires=requirements,
)
