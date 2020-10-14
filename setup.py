import setuptools

setuptools.setup(
    name='localedb',
    version='0.1',
    author='Tomek D. Loboda',
    author_email='tomek.loboda@gmail.com',
    description='A database of locales',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/momacs/localedb',
    keywords=['data', 'relational', 'database', 'locale', 'epidemiology'],
    packages=['localedb'],
    package_dir={'': 'src'},
    python_requires='>=3.6',
    install_requires=['psycopg2-binary'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering'
    ],
    license="BSD"
)
