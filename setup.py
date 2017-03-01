from setuptools import setup

setup(name='redisworker',
    version='0.1',
    description='Communicate jobs through redis',
    url='http://github.com/metricstory/pyredisworker',
    author='Will Schumacher',
    license='MIT',
    packages=['redisworker'],
    install_requires=[
        'redis'
    ],
    zip_safe=False)
