from setuptools import setup

setup(
    name='dropbox2s3',
    version='0.1',
    py_modules=['dropbox2s3'],
    include_package_data=True,
    install_requires=[
        'click',
    ],
    entry_points='''
        [console_scripts]
        dropbox2s3=dropbox2s3:cli
    ''',
)
