from setuptools import setup, find_packages
  
setup(
    name='CaJade',
    version='0.0.2',
    url='https://github.com/IITDBGroup/CaJaDe',
    author='Author Name',
    author_email='author@gmail.com',
    description='Description of my package',
    packages=find_packages(),
    install_requires=['seaborn==0.11.0',
                    'scipy==1.7.1',
                    'Flask==2.0.1',
                    'psycopg2-binary',
                    'varclushi==0.1.0',
                    'matplotlib==3.2.1',
                    'networkx==2.5',
                    'pandas==1.3.2',
                    'numpy==1.19.2',
                    'colorful==0.5.4',
                    'scikit_learn==1.0',
                    'causalgraphicalmodels'],
    entry_points={
        'console_scripts': [
            'cajadexplain=src.experiments:main',
        ]},
    )