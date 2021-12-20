from setuptools import setup
from setuptools import find_packages


def load(path):
    return open(path, 'r').read()


numerai_reports_version = '0.5.3'


classifiers = [
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Topic :: Scientific/Engineering"]


if __name__ == "__main__":
    setup(
        name="numerai_reports",
        version=numerai_reports_version,
        maintainer="uuazed",
        maintainer_email="uuazed@gmail.com",
        description="Reports for the Numerai machine learning competition",
        long_description=load('README.md'),
        long_description_content_type='text/markdown',
        url='https://github.com/uuazed/numerai_reports',
        platforms="OS Independent",
        classifiers=classifiers,
        license='MIT License',
        package_data={'numerai_reports': ['LICENSE', 'README.md']},
        packages=find_packages(exclude=['tests']),
        install_requires=["pandas", "numerapi", "pyarrow", "fsspec", "gcsfs"],
        )
