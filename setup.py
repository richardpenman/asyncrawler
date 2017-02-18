import os
from distutils.core import setup

def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()

setup(
    name='asyncrawler', 
    version='0.1',
    packages=['asyncrawler'],
    package_dir={'asyncrawler':'.'}, # look for package contents in current directory
    author='Richard Penman',
    author_email='richard@webscraping.com',
    description='Asyncio based web crawling framework',
    long_description=read('README.rst'),
    url='http://github.com/richardpenman/asyncrawler',
    classifiers = [
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP'
    ],
    license='lgpl'
)
