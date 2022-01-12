# flake8: noqa
from setuptools import setup


# File used to allow install from a branch
# ex: !pip3 install --upgrade git+https://github.com/zillow/datasets.git@tz/plugins

# To create this file
# poetry build --format sdist
# tar -xvf dist/*-`poetry version -s`.tar.gz -O '*/setup.py' > setup.py


packages = [
    "datasets",
    "datasets.plugins",
]

package_data = {"": ["*"]}

install_requires = [
    "importlib-metadata>=4.8.1,<5.0.0",
    "pandas>=1.1.0,<2.0.0",
    "pyarrow>=5.0.0,<6.0.0",
    "click>=7.0,<8",
]

extras_require = {
    ':extra == "dask"': ["dask>=2021.9.1,<2022.0.0"],
    ':extra == "spark"': ["pyspark>=3.0.0,<4.0.0"],
    ':extra == "metaflow"': ["zillow-metaflow @ " "git+https://github.com/zillow/metaflow.git@feature/kfp"],
}

entry_points = {
    "datasets.executors": ["metaflow_executor = datasets.plugins:MetaflowExecutor"],
    "datasets.plugins": [
        "batch_dataset = datasets.plugins:BatchDataset",
        "batch_flow_dataset = datasets.plugins:FlowDataset",
    ],
}

setup_kwargs = {
    "name": "zdatasets",
    "version": "0.0.1",
    "description": "Dataset SDK for consistent read/write [batch, online, streaming] data.",
    "author": "Taleb Zeghmi",
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "extras_require": extras_require,
    "entry_points": entry_points,
    "python_requires": ">=3.7,<4",
}


setup(**setup_kwargs)
