from setuptools import setup

setup(
    name='drop2s3',
    version='0.1',
    py_modules=['drop2s3'],
    include_package_data=True,
    install_requires=[
        'click',
        'boto3',
        'pandas',
        #'pathlib' # Included in python 3.4 and up
    ],
    entry_points='''
        [console_scripts]
        drop2s3=drop2s3:cli
    ''',
)
