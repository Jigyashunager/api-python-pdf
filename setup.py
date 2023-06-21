from setuptools import setup, find_packages

setup(
    name='MyProject',
    version='1.0',
    packages=['src'],
    install_requires=[
        'flask',
        'pyodbc',
        'pandas',
        'python-dotenv',
        'schedule',
        'flask-cors'
    ],
    # Additional build configuration options can be added here
)
