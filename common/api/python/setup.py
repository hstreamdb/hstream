import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hstreamdb-api",
    version="0.0.3",
    author="lambda",
    author_email="lambda@emqx.io",
    description="HStreamDB api for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hstreamdb/hstream",
    project_urls={
        "Bug Tracker": "https://github.com/hstreamdb/hstream/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "protobuf",
    ],
    python_requires=">=3.7",
)
