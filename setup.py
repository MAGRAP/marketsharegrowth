from setuptools import setup, find_packages

setup(
    name='market-growth-analysis',
    version='1.0.19',
    packages=['market_growth_analysis', 'market_growth_analysis.etl'],
    package_dir={'': 'src'},
    install_requires=[
        'requests',
        'beautifulsoup4',
        'tqdm',
        # Add any other dependencies your library requires
    ],
)
