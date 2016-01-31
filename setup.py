from setuptools import setup

# check for python 3.4.3 or equivalent

setup(
    name='BPCon',
    version='1.0',
    packages = find_packages(),
    install_requires=[
        "asyncio",
        "websockets",
        "pytest"
    ],
)
