from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    install_requires = f.read().splitlines()

setup(
    name='resync',
    description='An ORM-like wrapper for the rethinkdb asyncio driver',
    url='https://github.com/codiumco/resync',
    author='James Keys',
    author_email='james.k@cloudhm.co.th',
    packages=find_packages(exclude=('docs', 'tests')),
    include_package_data=True,
    install_requires=install_requires,
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
)
