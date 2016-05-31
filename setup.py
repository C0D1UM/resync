from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'LICENSE'), encoding='utf-8') as f:
    license = f.read()
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    install_requires = f.read().splitlines()

setup(
    name='resync-orm',
    license='BSD 3-clause',
    description='An ORM-like wrapper for the rethinkdb asyncio driver',
    long_description=long_description,
    url='https://github.com/codiumco/resync',
    author='James Keys',
    author_email='james.k@cloudhm.co.th',
    packages=find_packages(exclude=('docs', 'tests')),
    include_package_data=True,
    install_requires=install_requires,
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
    ],
    keywords='rethink rethinkdb asyncio',
)
