import setuptools
from setuptools import find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as reqs_file:
    requirements = reqs_file.readlines()

setuptools.setup(
    name="lithops-worker-pool",
    version="0.0.1",
    author="Aitor Arjona",
    author_email="aitor.arjona@urv.cat",
    description="A serverless worker pool built on top of Lithops framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aitorarjona/lithops-worker-pool",
    project_urls={
        "Bug Tracker": "https://github.com/aitorarjona/lithops-worker-pool/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta"
    ],
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.6",
)
