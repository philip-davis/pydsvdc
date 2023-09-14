from setuptools import setup

setup(
   name='pydsvdc',
   version='0.1',
   description='Metadata-based DataSpaces Queries',
   author='Philip Davis',
   author_email='philip.davis@sci.utah.edu',
   packages=['pydsvdc'],
   install_requires=['wheel', 'bitstring', 'numpy', 's3fs', 'pymongo',],
)
