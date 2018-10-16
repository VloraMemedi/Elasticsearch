from setuptools import setup, find_packages

install_requires = [
    'pandas',
    'csmodb',
    'numpy',
    'csmoai',
    'json',
    'datetime',
    'elasticsearch',
    'time']

setup(
    name='elasticevaluation',
    packages=find_packages(),
    install_requires=install_requires,
    url='',
    license='MIT',
    author='Vlora Memedi',
    author_email='vlora.memedi@visionspace.com',
    description='Elasticsearch evaluation',
    python_requires='>=3.3,<4'
)
