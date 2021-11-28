from setuptools import setup, find_packages
  
setup(
    name='CaJade',
    version='0.0.1',
    url='https://github.com/IITDBGroup/CaJaDe',
    author='Chenjie Li',
    author_email='cli112@hawk.iit.edu',
    description='Description of my package',
    packages=find_packages(),
    install_requires=['seaborn==0.11.0',
                    'scipy==1.5.2',
                    'Flask==2.0.1',
                    'psycopg2-binary==2.8.4',
                    'varclushi==0.1.0',
                    'matplotlib==3.2.1',
                    'networkx==2.5',
                    'pandas==1.3.2',
                    'numpy==1.19.2',
                    'colorful==0.5.4',
                    'scikit_learn==1.0'],
    entry_points={
        'console_scripts': [
            'cajadexplain=src.experiments:main',
        ]},
    )
