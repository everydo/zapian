################################
import os
from setuptools import setup, find_packages

def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

setup (
    name='zapian',
    version='0.4.0',
    author = "Pan Junyong",
    author_email = "dev@zopen.cn",
    description = "indexer addons: sort, cjksplitter, and so on",
    long_description=(
        read('README')
        ),
    license = "Private",
    keywords = "zope3 z3c rpc  server client",
    classifiers = [
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP',
        'Framework :: Zope3'],
    url = 'http://github.com/everydo/zapian',
    include_package_data = True,
    namespace_packages=[],
    packages = find_packages(),
    install_requires = [
        'setuptools',
        'PyYAML'
        ],
    zip_safe = False,
)
