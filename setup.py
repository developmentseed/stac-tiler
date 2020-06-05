"""Setup for stac-tiler."""

from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

# rasterio is installed via morecantile, so it's missing the [s3] option to install boto3
inst_reqs = ["rio-tiler-crs>=2.0.2", "requests", "boto3"]

extra_reqs = {
    "test": ["pytest", "pytest-cov"],
    "dev": ["pytest", "pytest-cov", "pre-commit"],
}

setup(
    name="stac-tiler",
    version="0.0rc.2",
    python_requires=">=3",
    description=u"""A rio-tiler plugin to handle STAC items""",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords="COG STAC GIS",
    author=u"Vincent Sarago",
    author_email="vincent@developmentseed.org",
    url="https://github.com/developmentseed/stac-tiler",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=inst_reqs,
    extras_require=extra_reqs,
)
