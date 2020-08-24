# LocaleDB

A database of U.S. locales.  Currently, the database supports epidemiological modeling and simulation is specifically tailored towards the COVID 19 pandemic.  Modeling in other disciplies may be supporeted in the future.


## Design

As shown on the figure below, LocaleDB stores several types of data (gray boxes indicate planned future extentions).  That data is stored in a PosgreSQL database which is managed by a command line tool ([`localedb`](localedb)).  The content of the database is accessed via a Python package which provides a high level API to, for example, suggest U.S. counties similar to the county specified.

<center><img height="526" alt="portfolio_view" src="media/design.png" /></center>

This design that separates data management and data consumption reflects the anticipated production use case.  Namely, the database will be deployed and set up once and will then require little to no manual management (periodic updates will be autonomous).  It will then be used for producing data that will drive modeling and simulation efforts.


## Dependencies: Database Server

LocaleDB can be deployed to a development and production environments.  It is recommended to familiarize yourself with the software using the development environment first.

### Development Environment

- curl or wget
- PostgreSQL client
- [Docker](https://www.docker.com)

### Production Environment

- curl or wget
- PostgreSQL client
- PostgreSQL server (with PostGIS extensions)
- [Python 3](https://www.python.org)

**Note**: LocaleDB should not be deployed to a production environment yet.  This note will be removed when that deployment mode has been fully implemented and fully tested.


## Dependencies: Python Package

- [Python 3](https://www.python.org)
- [psycopg2](https://pypi.org/project/psycopg2)


## Setup

### Command Line Management Tool

On MacOS run:

```
sh -c "$(curl -fsSL https://raw.githubusercontent.com/momacs/localedb/master/setup.sh -O -)"
```

On Linux run:
```
sh -c "$(wget -q https://raw.githubusercontent.com/momacs/localedb/master/setup.sh -O -)"
```

Alternatively, you can run the commands from the [`setup.sh`](setup.sh) script manually.

**Production environment:** For production deployment, after the installation script above has finished, edit the `$HOME/bin/localedb` script and change `is_prod=0` to `is_prod=1`.  This step is left to be done manually to ensure intent.

### Python Client Package

It is never a bad idea to first create a new Python virtual environment:

```
# sudo apt install python3-venv  # may be needed on Linux

python3 -m venv ./prj01
cd prj01
source ./bin/activate
```

Then, install the package like so:

```
pip install git+https://github.com/momacs/localedb.git
```


## Sample Usage

### CLI

After setting up the command line management tool, setup the LocaleDB instance, start it, and display some info by running the following three commands:

```
$ localedb setup
$ localedb start
$ localedb info
Environment
    Production  0
Directory structure
    Root                   /Users/tomek/.localedb            322M
    Runtime                /Users/tomek/.localedb/rt         0B
    PostgreSQL data        /Users/tomek/.localedb/pg         322M
    Geographic data        /Users/tomek/.localedb/dl/geo     0B
    Population data        /Users/tomek/.localedb/dl/pop     0B
    Disease dynamics data  /Users/tomek/.localedb/dl/disdyn  0B
    Intervention data      /Users/tomek/.localedb/dl/interv  0B
PostgreSQL server
    Hostname  localhost
    Port      5433
    Database  c19
    Username  postgres
    Password  sa
```

To see some basic database statistics (currently only record counts), run:

```
$ localedb db stats
geo
    st  0
    co  0
    tr  0
    bg  0
    bl  0
pop
    school     0
    hospital   0
    household  0
    gq         0
    workplace  0
    person     0
    gq_person  0
```

To import geographic and cartographic data for the state of Alaska, run:

```
$ localedb import geo AK
US states        done
US counties      done
AK tracts        done
AK block groups  done
AK blocks        done
Analyzing database... done
```

To import synthetic population data, run:

```
$ localedb import pop AK
AK  done
Analyzing database... done
```

Check the database statistics again:

```
$ localedb db stats
geo
    st  52
    co  3221
    tr  167
    bg  534
    bl  45292
pop
    school     459
    hospital   0
    household  258057
    gq         235
    workplace  32206
    person     681545
    gq_person  13130
```

Imported states can be removed like so:

```
localedb db rm state-geo AK
localedb db rm state-pop AK

localedb db rm state AK  # remove all data types
```

Once data has been imported and the downloaded data files are no longer needed, they can be removed like so:

```
localedb fs rm-data geo
localedb db rm-data pop

localedb db rm data-all  # remove all data files
```

To stop LocaleDB instance, run:

```
localedb stop
```

To uninstall LocaleDB (leaving nothing behind), run:

```
localedb uninstall
```

For the list of available commands, run `localedb`.  For an explanation of each command, run `localedb help`.  Keep in mind that some commands have subcommands.

### Python

Here is an example of the Python LocaleDB package can be used:

```python
from localedb import LocaleDB
db = LocaleDB()

db.set_pop_view_household('02')     # constrain view to households located in Alaska
print(db.get_pop_size())            # get size of population that lives in those households

db.set_pop_view_household('02013')  # do the same for one of the counties in Alaska
print(db.get_pop_size())
```

If the database is not installed on the localhost (or if any other connection parameters need to be adjusted), they should be passed to the `LocaleDB` class' constructor.  Documentation of the package will be published when more functionality is built into it.


## References

- [2010 U.S. Synthesized Population Dataset (MIDAS Program)](https://gitlab.com/momacs/dataset-pop-us-2010-midas)


## License
This project is licensed under the [BSD License](LICENSE.md).
