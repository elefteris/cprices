import re
from setuptools import setup

# Specify and open the version file
VERSION_FILE = "cprices/_version.py"
verstrline = open(VERSION_FILE, "rt").read()
print(verstrline)

# Automatically detect the package version from VERSION_FILE
VERSION_REGEX = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VERSION_REGEX, verstrline, re.M)
if mo:
    version_string = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSION_FILE,))


with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup(
    name='cprices',
    version=version_string,
    description='Consumer Prices Data Transformation Core Pipeline.',
    url='http://np2rvlapxx507.ons.statistics.gov.uk/EPDS/cprices',
    packages=[
        'cprices',
        'cprices.steps',
    ],
    zip_safe=False,
    install_requires=requirements
)
