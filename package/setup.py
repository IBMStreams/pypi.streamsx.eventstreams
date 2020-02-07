from setuptools import setup
import streamsx.eventstreams
setup(
    name = 'streamsx.eventstreams',
    packages = ['streamsx.eventstreams'],
    include_package_data=True,
    version = streamsx.eventstreams.__version__,
    description = 'IBM Streams Event Streams integration for IBM Streams topology applications',
    long_description = open('DESC.txt').read(),
    author = 'IBM Streams @ github.com',
    author_email = 'rolef.heinrich@de.ibm.com',
    license='Apache License - Version 2.0',
    url = 'https://github.com/IBMStreams/pypi.streamsx.eventstreams',
    keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'messaging', 'eventstreams', 'events'],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=[
        'streamsx>=1.12.10',
        'streamsx.toolkits'
        ],
    
    test_suite='nose.collector',
    tests_require=['nose']
)
