import os
from setuptools import setup, find_packages


setup(
    name='bosscat',
    version=0.1,
    author='Code Hat Labs, LLC',
    author_email='dev@codehatlabs.com',
    url='https://github.com/eightup/bosscat',
    description='Automated Python deployments to AWS Cloud',
    packages=find_packages(),
    long_description="",
    keywords='python',
    zip_safe=False,
    install_requires=[
    ],
    test_suite='',
    include_package_data=True,
    classifiers=[
        'Framework :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
