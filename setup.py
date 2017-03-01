from setuptools import setup

setup(name='redisworker',
    version='0.1',
    description='Communicate jobs through redis',
    url='https://github.com/metricstory/pyredisworker',
    download_url='https://github.com/metricstory/redisworker/archive/0.1.tar.gz',
    author='Will Schumacher',
    author_email='will@metricstory.com',
    license='MIT',
    packages=['redisworker'],
    install_requires=[
        'redis'
    ],
    keywords=['redis'])
