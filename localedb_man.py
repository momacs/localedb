# -*- coding: utf-8 -*-
"""LocaleDB management routines."""

import datetime
import csv
import io
import math
import numpy as np
import os
import pandas as pd
import psycopg2
import psycopg2.extras
import re
import sys
import time
import urllib.request
from ftplib import FTP
import itertools
import random as rd

from abc         import ABC
from collections import namedtuple
from pathlib     import Path
from sqlalchemy  import create_engine

from psycopg2.errors import UniqueViolation
from sqlalchemy.exc  import IntegrityError


# ----------------------------------------------------------------------------------------------------------------------
def req_argn(n):
    """Requires the specified number of command line arguments passed to the script."""

    if len(sys.argv[1:]) != n:
        print(f'Incorrect number of arguments; expected {n} but {len(sys.argv[1:])} provided.')
        sys.exit(1)


# ----------------------------------------------------------------------------------------------------------------------
class ETLError(Exception): pass


# ----------------------------------------------------------------------------------------------------------------------
class DBI(object):
    """Database interface.
    """

    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop, pg_schema_vax, pg_schema_health, pg_schema_weather, pg_schema_mobility, cursor_factory=psycopg2.extras.NamedTupleCursor):
        self.pg_host        = pg_host
        self.pg_port        = pg_port
        self.pg_usr         = pg_usr
        self.pg_pwd         = pg_pwd
        self.pg_db          = pg_db
        self.pg_schema_dis  = pg_schema_dis
        self.pg_schema_geo  = pg_schema_geo
        self.pg_schema_main = pg_schema_main
        self.pg_schema_pop  = pg_schema_pop
        self.pg_schema_vax  = pg_schema_vax
        self.pg_schema_health  = pg_schema_health
        self.pg_schema_weather  = pg_schema_weather
        self.pg_schema_mobility  = pg_schema_mobility

        self.conn = psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_usr, password=self.pg_pwd, database=self.pg_db, cursor_factory=cursor_factory)

    def __del__(self):
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()
            self.conn = None

    def is_col(self, col, tbl, schema=None, cursor=None):
        """Does the column specified exist?

        Args:
            col (str): Column name.
            tbl (str): Table name.
            schema (str, optional): Schema name.
            cursor (Cursor, optional): DB cursor.

        Returns:
            bool: True if the column exists; False otherwise.

        If both the ``cursor`` and ``shema`` are ``None``, the ``public`` schema is assumed.  If only ``schema`` is
        ``None``, a temporary table is assumed and its namespace is used automatically.
        """

        if schema is None:
            if cursor is None:
                schema = "'public'"
            else:
                schema = 'pg_my_temp_schema()'
        else:
            schema = f"'{schema}'"
        if cursor is None:
            cursor = self.conn.cursor()
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = {schema} AND table_name = '{tbl}' AND column_name = '{col}');")
        return cursor.fetchone()[0]

    def vacuum(self, tbl=None, do_full=False):
        self.conn.autocommit = True
        c = self.conn.cursor()
        c.execute(f'VACUUM (FULL, ANALYZE) {tbl or ""};' if do_full else f'VACUUM ANALYZE {tbl or ""};')
        c.close()
        self.conn.autocommit = False


# ----------------------------------------------------------------------------------------------------------------------
class FSI(object):
    """Filesystem interface.
    """

    def __init__(self, dpath_log, dpath_rt):
        self.dpath_log = Path(dpath_log)
        self.dpath_rt  = Path(dpath_rt)  # runtime dir assumed to contain the source files uncompressed and ready for processing

        self.log = None  # log file; open upon request elsewhere

    def __del__(self):
        if hasattr(self, 'log') and self.log is not None:
            self.log.close()
            self.log = None

    def get_log(self):
        if self.log is None:
            self.log = open(os.path.join(self.dpath_log, datetime.datetime.now().strftime('%Y-%m-%d %H.%M.%S.%f')), 'w')
        return self.log


# ----------------------------------------------------------------------------------------------------------------------
class Schema(ABC):
    """Database schema manager.

    Manages one type of data type.  Data type are compartmentalized into PostgreSQL's schemas.
    """

    def __init__(self, dbi, fsi, engine=None):
        self.dbi = dbi
        self.fsi = fsi
        self.engine = engine


# ----------------------------------------------------------------------------------------------------------------------
class DiseaseSchema(Schema):
    """Diseases (dynamics, non-pharmaceutical interventions, medical countermeasures, etc.).

    Pandemics
        2009 H1N1 (H1N1pdm09 virus)
        1968 H3N2 (H3N2 virus)
        1957 H2N2 (H2N2 virus)
        1918 H1N1 (H1N1 virus)

    Epidemics
        2003 SARS

    Medical countermeasures (MCMs) can include:
        - Biologic products, such as vaccines, blood products and antibodies
        - Drugs, such as antimicrobial or antiviral drugs
        - Devices, including diagnostic tests to identify threat agents, and personal protective equipment (PPE), such
          as gloves, respirators (face masks), and ventilators

    Data sources
        Disease dynamics
            COVID-19: https://github.com/CSSEGISandData/COVID-19
        Vaccinations
            2010-11 through 2018-19 Influenza Seasons Vaccination Coverage Trend Report
                https://www.cdc.gov/flu/fluvaxview/reportshtml/trends/index.html
        Non-pharmaceutical interventions)
            https://www.keystonestrategy.com/coronavirus-covid19-intervention-dataset-model/
                https://github.com/Keystone-Strategy/covid19-intervention-data
                - Only US
                - Only 650 US counties covered as of 6/18/2020 (additions in progress supposedly)
                    States and counties list: https://docs.google.com/spreadsheets/d/1gSpwUsPtuzKPvXLVcytFFHJANmOxRUzKp8ytYGvx72w/edit#gid=214592991
                * Good for US data
            https://www.nature.com/articles/s41597-020-00609-9
                https://github.com/amel-github/covid19-interventionmeasures
                https://drive.google.com/drive/folders/1041U8iWPDSGI6KHIn9Dg7THkXIo3-gui
                - Only a handful of US states covered
                + Many countries are listed
                * Good for international data
    """

    URL_DYN_COVID_19_DEAD_GLOB = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    URL_DYN_COVID_19_DEAD_US   = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
    URL_DYN_COVID_19_CONF_GLOB = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    URL_DYN_COVID_19_CONF_US   = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
    URL_DYN_COVID_19_REC_GLOB  = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
    URL_DYN_COVID_19_REC_US    = None  # dataset not available

    URL_NPI_COVID_19_KEYSTONE = 'https://raw.githubusercontent.com/Keystone-Strategy/covid19-intervention-data/master/complete_npis_inherited_policies.csv'

    def load_disease(self, disease):
        {
            'COVID-19' : self.load_covid_19,
            'H1N1'     : self.load_h1n1
        }.get(disease, lambda: print(f'Unknown disease: {disease}'))()

    def load_covid_19(self):
        disease_id = None
        with self.dbi.conn.cursor() as c:
            c.execute(f"INSERT INTO {self.dbi.pg_schema_dis}.disease (name) VALUES ('COVID-19') ON CONFLICT DO NOTHING;")
            c.execute(f'SELECT id FROM {self.dbi.pg_schema_dis}.disease WHERE name = %s;', ['COVID-19'])
            disease_id = c.fetchone()[0]
        self.dbi.conn.commit()

        self.load_covid_19_dyn(disease_id)
        self.load_covid_19_npi(disease_id)

    def load_covid_19_dyn(self, disease_id):
        print(f'Disease dynamics', flush=True)

        with self.dbi.conn.cursor() as c:
            c.execute(f'CREATE TEMPORARY TABLE dyn_load (LIKE {self.dbi.pg_schema_dis}.dyn INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);')

            self.load_covid_19_dyn_ds(c, disease_id, self.URL_DYN_COVID_19_CONF_GLOB, 'n_conf', 'confirmed', True,  date_col_idx_0=4)
            self.load_covid_19_dyn_ds(c, disease_id, self.URL_DYN_COVID_19_DEAD_GLOB, 'n_dead', 'deaths',    True,  date_col_idx_0=4)
            self.load_covid_19_dyn_ds(c, disease_id, self.URL_DYN_COVID_19_REC_GLOB,  'n_rec',  'recovered', True,  date_col_idx_0=4)

            self.load_covid_19_dyn_ds(c, disease_id, self.URL_DYN_COVID_19_CONF_US,   'n_conf', 'confirmed', False, date_col_idx_0=12)
            self.load_covid_19_dyn_ds(c, disease_id, self.URL_DYN_COVID_19_DEAD_US,   'n_dead', 'deaths',    False, date_col_idx_0=12)

            print(f'    Consolidating...', end='', flush=True)
            t0 = time.perf_counter()
            c.execute(f'DELETE FROM {self.dbi.pg_schema_dis}.dyn WHERE disease_id = %s;', [disease_id])
            c.execute(f'INSERT INTO {self.dbi.pg_schema_dis}.dyn SELECT * FROM dyn_load;')
        self.dbi.conn.commit()
        self.dbi.vacuum(f'{self.dbi.pg_schema_dis}.dyn')
        print(f' done ({time.perf_counter() - t0:.0f} s)', flush=True)

    def load_covid_19_dyn_ds(self, c, disease_id, url, col, col_human, is_glob, date_col_idx_0, page_size=1024):
        print(f'    Loading {"global" if is_glob else "US"} {col_human}...', end='', flush=True)
        t0 = time.perf_counter()

        # (1) Extract:
        res = urllib.request.urlopen(url)
        reader = csv.reader([l.decode('utf-8') for l in res.readlines()])
        header = next(reader)

        # (2) Transform:
        rows = [[None if c == '' else c for c in r] for r in reader]

        # (3) Load:
        for (i,r) in enumerate(rows):
            if is_glob:
                c.execute(f'SELECT id FROM {self.dbi.pg_schema_main}.locale WHERE admin0 IS NOT DISTINCT FROM %s AND admin1 IS NOT DISTINCT FROM %s AND admin2 IS NULL;', [r[1], r[0]])
            else:
                c.execute(f'SELECT id FROM {self.dbi.pg_schema_main}.locale WHERE id = %s;', [r[0]])
            locale_id = c.fetchone()
            if locale_id is None:
                print(f'Locale not been found in the database: {r[:7]}')
                self.dbi.conn.rollback()
                sys.exit(1)
            locale_id = locale_id[0]

            psycopg2.extras.execute_batch(c,
                f'INSERT INTO dyn_load AS d (disease_id, locale_id, day, day_i, {col}) VALUES (%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT dyn_load_pkey DO ' +
                f'UPDATE SET {col} = %s WHERE d.disease_id = %s AND d.locale_id = %s AND d.day = %s',
                ([disease_id, locale_id, header[j], j - date_col_idx_0 + 1, r[j], r[j], disease_id, locale_id, header[j]] for j in range(date_col_idx_0, len(r))),
                page_size=page_size
            )
        print(f' done ({time.perf_counter() - t0:.0f} s)', flush=True)

    def load_covid_19_npi(self, disease_id):
        print(f'Non-pharmaceutical interventions', flush=True)

        self.load_covid_19_npi_keystone(disease_id)

    def load_covid_19_npi_keystone(self, disease_id):
        print(f'    Loading Keystone...', end='', flush=True)
        t0 = time.perf_counter()

        # (1) Extract:
        res = urllib.request.urlopen(self.URL_NPI_COVID_19_KEYSTONE)
        reader = csv.reader([l.decode('utf-8') for l in res.readlines()])
        header = next(reader)

        # (2) Transform:
        rows = [[None if c == '' else c for c in r] for r in reader if r[4] != '']
            # - convert empty strings to None (the way CSV should function)
            # - exclude rows with empty start-date

        types = {}
        with self.dbi.conn.cursor() as c:
            for (i,r) in enumerate(rows):
                # Remove erronous values appearing in columns 6-9:
                for j in [6,7,8,9]:
                    if r[j] is not None and r[j].lower() in ['t', 'f', 'true', 'false']:
                        r[j] = None

                # Correct encoding error (https://github.com/Keystone-Strategy/covid19-intervention-data/issues/19):
                if r[0] == '35013':
                    r[1] = 'Dona Ana'

                # Make names of intervention types more palletable and persist them (they become primary/foreign keys):
                r[3] = r[3].replace('_', ' ')
                if r[3] not in types.keys():
                    types[r[3]] = len(types)
                r[3] = types[r[3]]

                # Link with the 'main.locale' table:
                c.execute(f"SELECT id FROM {self.dbi.pg_schema_main}.locale WHERE admin0 = 'US' AND admin1 = %s AND admin2 IS NOT DISTINCT FROM %s;", [r[2], r[1]])
                rr = c.fetchall()
                if len(rr) != 1:
                    raise ETLError(f'ETL error: Exactly one locale expected but {len(rr)} found for line {i} that starts with: {r[0], r[2], r[1]}')
                r.append(rr[0][0])
        rows = list(dict.fromkeys([tuple(r) for r in rows]))  # remove duplicates (TODO: Doesn't currently work therefore the ON CONFLICT work around below)

        # (3) Load:
        with self.dbi.conn.cursor() as c:
            c.execute(f'DELETE FROM {self.dbi.pg_schema_dis}.npi;')
            c.execute(f'DELETE FROM {self.dbi.pg_schema_dis}.npi_type;')

            psycopg2.extras.execute_batch(c,
                f'INSERT INTO {self.dbi.pg_schema_dis}.npi_type (id, name) VALUES (%s,%s);',
                ((v,k) for (k,v) in types.items())
            )
            psycopg2.extras.execute_batch(c,
                f'INSERT INTO {self.dbi.pg_schema_dis}.npi (disease_id, locale_id, type_id, begin_date, end_date, begin_citation, begin_note, end_citation, end_note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;',
                ((disease_id, r[10], r[3], r[4], r[5], r[6], r[7], r[9], r[8]) for r in rows)
            )
        self.dbi.conn.commit()
        self.dbi.vacuum(f'{self.dbi.pg_schema_dis}.npi')
        print(f' done ({time.perf_counter() - t0:.0f} s)', flush=True)

    def load_h1n1(self):
        pass


# ----------------------------------------------------------------------------------------------------------------------
class MainSchema(Schema):
    URL_LOCALES_JHU = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'

    def load_locales(self):
        self.load_locales_jhu()
        self.load_locales_geonames()

    def load_locales_geonames(self):
        pass

    def load_locales_jhu(self):
        # (1) Extract:
        res = urllib.request.urlopen(self.URL_LOCALES_JHU)
        reader = csv.reader([l.decode('utf-8') for l in res.readlines()])
        header = next(reader)

        # (2) Transform:
        rows = [[None if c == '' else c for c in r] for r in reader]

        # (3) Load:
        with self.dbi.conn.cursor() as c:
            c.execute(f'DELETE FROM {self.dbi.pg_schema_main}.locale;')
            psycopg2.extras.execute_batch(c,
                f'INSERT INTO {self.dbi.pg_schema_main}.locale (id, iso2, iso3, iso_num, fips, admin0, admin1, admin2, lat, long, pop) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                ((r[0], r[1], r[2], r[3], r[4], r[7], r[6], r[5], r[8], r[9], r[11]) for r in rows)
            )
        self.dbi.conn.commit()
        self.dbi.vacuum(f'{self.dbi.pg_schema_main}.locale')


# ----------------------------------------------------------------------------------------------------------------------
class HealthSchema(Schema):
    """Health factors and outcomes.

    National Health Interview Survey (NHIS): https://www.cdc.gov/nchs/nhis/index.htm?CDC_AA_refVal=https%3A%2F%2Fwww.cdc.gov%2Fnchs%2Fnhis.htm
    Behavioral Risk Factor Surveillance System (BRFSS): https://www.cdc.gov/brfss/
    """

    pass


# ----------------------------------------------------------------------------------------------------------------------
class PopSchema(Schema):
    """2010 US synthesized population (MIDAS Program)

    Data mirror: https://gitlab.com/momacs/dataset-pop-us-2010-midas
    """

    CountyTxtFile = namedtuple('CountyTxtFile', ('fname', 'tbl', 'ln_re', 'copy_sql', 'upd_coords_col'))

    NA = 'X'  # missing value string

    SQL_CREATE_TEMP_TABLES = '''
        CREATE TEMP TABLE tmp_school    ON COMMIT DROP AS TABLE {schema}.school    WITH NO DATA;
        CREATE TEMP TABLE tmp_hospital  ON COMMIT DROP AS TABLE {schema}.hospital  WITH NO DATA;
        CREATE TEMP TABLE tmp_household ON COMMIT DROP AS TABLE {schema}.household WITH NO DATA;
        CREATE TEMP TABLE tmp_gq        ON COMMIT DROP AS TABLE {schema}.gq        WITH NO DATA;
        CREATE TEMP TABLE tmp_workplace ON COMMIT DROP AS TABLE {schema}.workplace WITH NO DATA;
        CREATE TEMP TABLE tmp_person    ON COMMIT DROP AS TABLE {schema}.person    WITH NO DATA;
        CREATE TEMP TABLE tmp_gq_person ON COMMIT DROP AS TABLE {schema}.gq_person WITH NO DATA;
    '''

    COUNTY_TXT_FILES = [
        CountyTxtFile('schools.txt',    'school',    (False, re.compile(r'^\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')),           f"COPY tmp_school    (id, stco, lat, long)                                       FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('hospitals.txt',  'hospital',  (True,  re.compile(r'^\d+\t\d+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_hospital  (id, worker_cnt, physician_cnt, bed_cnt, lat, long)                       FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('households.txt', 'household', (False, re.compile(r'^\d+\t\d+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_household (id, stcotrbg, race_id, income, lat, long)                  FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('gq.txt',         'gq',        (False, re.compile(r'^\d+\t\w+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_gq        (id, type, stcotrbg, person_cnt, lat, long)                 FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('workplaces.txt', 'workplace', (False, re.compile(r'^\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')),                f"COPY tmp_workplace (id, lat, long)                                                           FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('people.txt',     'person',    (False, re.compile(r'^\d+\t\d+\t\d+\t[FM]\t\d+\t\d+\t(?:\d+|X)\t(?:\d+|X)$')),    f"COPY tmp_person    (id, household_id, age, sex, race_id, relate_id, school_id, workplace_id) FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", False),
        CountyTxtFile('gq_people.txt',  'gq_person', (False, re.compile(r'^\d+\t\d+\t\d+\t[FM]$')),                                    f"COPY tmp_gq_person (id, gq_id, age, sex)                                                     FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", False)
    ]

    def load_state(self, st_fips):
        """Loads a state to the database.

        The state ZIP file is expected to have been uncompressed to the self.fsi.dpath_rt directory.
        """

        log = self.fsi.get_log()
        with self.dbi.conn.cursor() as c:
            c.execute(self.__class__.SQL_CREATE_TEMP_TABLES.format(schema=self.dbi.pg_schema_pop))
            c.execute('SET CONSTRAINTS ALL DEFERRED;')
            for ctf in self.__class__.COUNTY_TXT_FILES:
                self.load_county_txt_files(c, ctf, st_fips, log)
        self.dbi.conn.commit()

    def load_county_txt_files(self, c, county_txt_file, st_fips, log):
        """Process data from the specified county-level file; file of this types for all counties are processed at the
        same time.

        This method uses one or two file objects.  If no content filtering is to be done, the data file is opened and
        used directly without censoring.  Otherwise, a StringIO object is used to store those data file lines that
        pass the filtering.  Hence, in the second case, two file objects are used.

        All this complexity is necessary to clean up the data because as it turns out the synthetic population data is
        plagued with significant problems (e.g., negative household income, non-number geo-coordinates, and shifted
        records).

        The synthetic population uses the WGS 84 standard (i.e., srid = 4326) while the US Census Bureau uses srid of
        4269 for the geographic and cartographic data.  This method applies the transform from population to geographic
        data.  Additionally, a geometry index is created.
        """

        for path_file in self.fsi.dpath_rt.rglob(county_txt_file.fname):
            if os.path.getsize(path_file) == 0:
                continue

            with open(path_file, 'r') as f01, io.StringIO() as f02:
                next(f01)

                log.write(f'{str(path_file)}\n')
                if county_txt_file.ln_re[0]:
                    for ln in f01:
                        if county_txt_file.ln_re[1].match(ln):
                            f02.write(ln)
                        else:
                            log.write(ln)
                    f02.seek(0)
                else:
                    f02 = f01

                c.copy_expert(county_txt_file.copy_sql.format(schema=self.dbi.pg_schema_pop), f02)
                tbl = county_txt_file.tbl

                # Store the state FIPS code:
                # c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{tbl}' AND column_name = 'st_fips');")
                # if bool(c.fetchone()[0]):
                #     c.execute(f"UPDATE tmp_{tbl} SET st_fips = '{st_fips}';")

                # Link with the 'main.locale' table:
                # c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{tbl}' AND column_name = 'st_id');")
                # if bool(c.fetchone()[0]):
                if self.dbi.is_col('st_id', f'{tbl}', self.dbi.pg_schema_pop):
                    # c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{tbl}' AND column_name = 'stco');")
                    # if bool(c.fetchone()[0]):
                    if self.dbi.is_col('stco', f'{tbl}', self.dbi.pg_schema_pop):
                        # c.execute(f'UPDATE tmp_{tbl} SET st_id = (SELECT l.id FROM main.locale l LEFT JOIN pop.household h ON l.fips = substring(h.stcotrbg from 1 for 2));')
                        # c.execute(f'UPDATE tmp_{tbl} SET co_id = (SELECT l.id FROM main.locale l LEFT JOIN pop.household h ON l.fips = substring(h.stcotrbg from 3 for 3));')
                        c.execute(f'UPDATE tmp_{tbl} x SET st_id = l.id FROM main.locale l WHERE l.fips = substring(x.stco from 1 for 2);')
                        c.execute(f'UPDATE tmp_{tbl} x SET co_id = l.id FROM main.locale l WHERE l.fips = substring(x.stco from 1 for 5);')
                    # c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{tbl}' AND column_name = 'stcotrbg');")
                    # if bool(c.fetchone()[0]):
                    if self.dbi.is_col('stcotrbg', f'{tbl}', self.dbi.pg_schema_pop):
                        # c.execute(f'UPDATE tmp_{tbl} SET st_id = (SELECT l.id FROM main.locale l LEFT JOIN pop.household h ON l.fips = substring(h.stcotrbg from 1 for 2));')
                        # c.execute(f'UPDATE tmp_{tbl} SET co_id = (SELECT l.id FROM main.locale l LEFT JOIN pop.household h ON l.fips = substring(h.stcotrbg from 3 for 3));')
                        c.execute(f'UPDATE tmp_{tbl} x SET st_id = l.id FROM main.locale l WHERE l.fips = substring(x.stcotrbg from 1 for 2);')
                        c.execute(f'UPDATE tmp_{tbl} x SET co_id = l.id FROM main.locale l WHERE l.fips = substring(x.stcotrbg from 1 for 5);')

                # Update the GEOM column and transform to the target srid:
                if county_txt_file.upd_coords_col:
                    c.execute(f"UPDATE tmp_{tbl} SET coords = ST_GeomFromText('POINT(' || long || ' ' || lat || ')', 4326) WHERE lat != 0 AND long != 0;")
                    c.execute(f'UPDATE tmp_{tbl} SET coords = ST_Transform(coords, 4269) WHERE lat != 0 AND long != 0;')

                # Populate the destination table:
                c.execute(f'INSERT INTO {self.dbi.pg_schema_pop}.{tbl} SELECT * FROM tmp_{tbl} ON CONFLICT DO NOTHING;')
                c.execute(f'TRUNCATE tmp_{tbl};')

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_pop}.school;')
            print(c.fetchone().n)


# ----------------------------------------------------------------------------------------------------------------------
class VaxSchema(Schema):
    """2010-11 through 2018-19 Influenza Seasons Vaccination Coverage Trend Report
    Original data available here: https://www.cdc.gov/flu/fluvaxview/reportshtml/trends/index.html

    Data mirror: https://world-modelers.s3.amazonaws.com/data/fluvax/CDC_Fluvax.xlsx
    """

    def process_df_(self, inner_df, df, age=None, race=None):
        """
        Processes intermediate vaccination dataframe
        """
        proc_dfs = pd.DataFrame()
        iters = int(inner_df.shape[1]/6)
        for i in range(iters):
            df_ = inner_df.iloc[:,i*6:(i+1)*6]
            date = df_.columns[0].split('.')[0]
            df_.columns = ['COVERAGE','LL','UL','CI','SAMPLE','TARGET']
            df_['DATE'] = date
            df_ = pd.concat([df.iloc[:,0:1],df_], axis=1)
            df_['AGE'] = age
            df_['RACE'] = race
            proc_dfs = proc_dfs.append(df_)
        return proc_dfs

    def parse_CI(self, CI):
        """
        Parses confidence interval into a float
        """
        if not pd.isna(CI):
            return float(CI.split('±')[1].split(')')[0])
        else:
            return CI

    def age_parser(self, row):
        age_lookup = {'13-17 years': {'age_0': 13, 'age_1': 17, 'is_high_risk': False},
                     '18-49 years': {'age_0': 18, 'age_1': 49, 'is_high_risk': False},
                     '18-49 years at high risk': {'age_0': 18, 'age_1': 49, 'is_high_risk': True},
                     '18-49 years not at high risk': {'age_0': 18, 'age_1': 49, 'is_high_risk': False},
                     '18-64 years': {'age_0': 18, 'age_1': 64, 'is_high_risk': False},
                     '18-64 years at high risk': {'age_0': 18, 'age_1': 64, 'is_high_risk': True},
                     '18-64 years not at high risk': {'age_0': 18, 'age_1': 64, 'is_high_risk': False},
                     '5-12 years': {'age_0': 5, 'age_1': 12, 'is_high_risk': False},
                     '50-64 years': {'age_0': 50, 'age_1': 64, 'is_high_risk': False},
                     '6 months - 17 years': {'age_0': 0.5, 'age_1': 17, 'is_high_risk': False},
                     '6 months - 4 years': {'age_0': 0.5, 'age_1': 4, 'is_high_risk': False},
                     '≥18 years': {'age_0': 18, 'age_1': np.nan, 'is_high_risk': False},
                     '≥6 months': {'age_0': 0.5, 'age_1': np.nan, 'is_high_risk': False},
                     '≥65 years': {'age_0': 65, 'age_1': np.nan, 'is_high_risk': False}
                     }
        match = age_lookup[row['name']]
        row['age_0'] = match['age_0']
        row['age_1'] = match['age_1']
        row['is_high_risk'] = match['is_high_risk']
        return row

    def get_locales(self, df):
        locale_df = pd.read_sql("SELECT id, admin1 FROM main.locale WHERE admin0='US' AND admin2 IS NULL;", self.engine)
        df = df.join(locale_df.set_index('admin1'), how='inner', on='locale')
        df = df.rename(columns = {'id': 'locale_id'})
        del(df['locale'])
        return df

    def process_vax_file(self):
        """
        Major preprocessing of vaccination data from CDC.
        This function performs a major pivot on the CDC data to convert it from a human-readable
        spreadsheet into a nicely formatted Pandas dataframe.
        """
        df = pd.read_excel('CDC_Fluvax.xlsx', skiprows=2)
        df_headers = pd.read_excel('CDC_Fluvax.xlsx')[:1]
        cols_1 = list(df_headers.columns)[1:]
        cols_2 = list(df_headers.iloc[0])[1:]

        cols_1_ = []
        cols_1_inds = {}
        count = 0
        for i in cols_1:
            if 'Unnamed' in i:
                cols_1_.append(None)
            else:
                cols_1_.append(i)
                cols_1_inds[count] = i
            count += 1

        cols_2_ = []
        cols_2_inds = {}
        count = 0
        for i in cols_2:
            if pd.isna(i):
                cols_2_.append(None)
            else:
                cols_2_.append(i)
                cols_2_inds[count] = i
            count += 1

        df_age = df.iloc[:,:721]
        df_race = pd.concat([df.iloc[:,0:1],df.iloc[:,721:]], axis=1)
        ages_l = list(cols_2_inds.items())[:15]
        race_l = list(cols_2_inds.items())[14:]
        race_l.append((df.shape[1], None))

        age_frames = {}
        for i in range(len(ages_l)-1):
            age = ages_l[i][1]
            start = ages_l[i][0] + 1
            end = ages_l[i + 1][0] + 1
            df_ = df_age.iloc[:, start:end]
            age_frames[age] = df_

        race_frames = {}
        for i in range(len(race_l)-1):
            race = race_l[i][1]
            start = race_l[i][0] + 1 - 720
            end = race_l[i + 1][0] + 1 - 720
            df_ = df_race.iloc[:, start:end]
            race_frames[race] = df_

        out_df = pd.DataFrame()
        for kk, vv in age_frames.items():
            interim_df = self.process_df_(vv, df, age=kk)
            out_df = out_df.append(interim_df)

        for kk, vv in race_frames.items():
            interim_df = self.process_df_(vv, df, race=kk)
            out_df = out_df.append(interim_df)

        out_df = out_df.rename(columns={'Names':'LOCALE'})
        out_df['START_YEAR'] = out_df['DATE'].apply(lambda x: int(x.split('-')[0]))
        out_df['END_YEAR'] = out_df['DATE'].apply(lambda x: int('20'+ x.split('-')[1]))
        del(out_df['DATE'])

        # Convert AGE and RACE to categorical variables
        out_df['AGE'] = pd.Categorical(out_df.AGE)
        out_df['AGE_ID'] = out_df.AGE.cat.codes

        out_df['RACE'] = pd.Categorical(out_df.RACE)
        out_df['RACE_ID'] = out_df.RACE.cat.codes

        out_df['RACE_ID'] = out_df.RACE_ID.replace(-1, np.nan)
        out_df['AGE_ID'] = out_df.AGE_ID.replace(-1, np.nan)

        # Generate lookup tables
        age_cats = []
        for kk, vv in dict( enumerate(out_df.AGE.cat.categories )).items():
            age_cats.append({'id': kk, 'name': vv})

        race_cats = []
        for kk, vv in dict( enumerate(out_df.RACE.cat.categories )).items():
            race_cats.append({'id': kk, 'name': vv})

        age_cats_df = pd.DataFrame(age_cats)
        race_cats_df = pd.DataFrame(race_cats)

        del(out_df['AGE'])
        del(out_df['RACE'])

        out_df.columns= out_df.columns.str.lower()
        age_cats_df.columns= age_cats_df.columns.str.lower()
        race_cats_df.columns= race_cats_df.columns.str.lower()

        # replace NR values with null
        out_df = out_df.replace(to_replace='.*NR.*', value=np.nan, regex=True)

        # parse CI field
        out_df['ci'] = out_df.ci.apply(lambda x: self.parse_CI(x))

        # update age table with quantitative lookups
        age_cats_df = age_cats_df.apply(lambda x: self.age_parser(x), axis=1)

        # map locales to main schema
        out_df = self.get_locales(out_df)

        return out_df, age_cats_df, race_cats_df

    def load_vax(self):
        """
        Loads Flu vaccine data to database.
        Uses Pandas and SQLAlchemy. If the data is already in the database, it alerts the user.
        """
        urllib.request.urlretrieve('https://world-modelers.s3.amazonaws.com/data/fluvax/CDC_Fluvax.xlsx', 'CDC_Fluvax.xlsx')
        data, age, race = self.process_vax_file()
        try:
            age.to_sql('age', con=self.engine, schema=self.dbi.pg_schema_vax, index=False, if_exists='append')
            race.to_sql('race', con=self.engine, schema=self.dbi.pg_schema_vax, index=False, if_exists='append')
            data.to_sql('vax', con=self.engine, schema=self.dbi.pg_schema_vax, index=False, if_exists='append')
        except IntegrityError as e:
            assert isinstance(e.orig, UniqueViolation)
            print("Vaccination data is already loaded in LocaleDB.")

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_vax}.vax;')
            print(c.fetchone().n)


# ----------------------------------------------------------------------------------------------------------------------
class HealthSchema(Schema):
    """County Health Rankings 2020 Health Trends from https://www.countyhealthrankings.org/explore-health-rankings/rankings-data-documentation
    Original data available here: https://www.countyhealthrankings.org/sites/default/files/media/document/CHR_trends_csv_2020.csv

    Data mirror: https://world-modelers.s3.amazonaws.com/data/CHR/CHR_trends_csv_2020.csv
    """

    def format_row(self, row):
        """
        Fixes start_year and end_year
        also formats FIPS column
        """
        if len(row.yearspan)>4:
            row['start_year'] = int(row.yearspan.split('-')[0])
            row['end_year'] = int(row.yearspan.split('-')[1])        
        else:
            row['start_year'] = int(row.yearspan)
            row['end_year'] = int(row.yearspan)

        st = str(row.statecode)
        ct = str(row.countycode)
        if len(st) == 1:
            st = '0'+ st
        if len(ct) == 1:
            ct = '00' + ct
        elif len(ct) == 2:
            ct = '0' + ct
        

        if st == '00': # for all of US
            row['fips'] = '840'

        elif ct == '000': # for a state
            row['fips'] = '000' + st
        else: # for a county
            row['fips'] = st + ct
        return row        

    def get_locales(self, df):
        locale_df = pd.read_sql("SELECT id, fips FROM main.locale where admin0='US'", self.engine)
        df = df.join(locale_df.set_index('fips'), how='inner', on='fips')
        df = df.rename(columns = {'id': 'locale_id'})
        del(df['fips'])
        return df

    def process_health_file(self, st_fips):
        """
        Major preprocessing of vaccination data from CDC.
        This function performs a major pivot on the CDC data to convert it from a human-readable
        spreadsheet into a nicely formatted Pandas dataframe.
        """
        df = pd.read_csv("CHR_trends_csv_2020.csv", encoding = "ISO-8859-1", thousands=",", low_memory=False)
        if st_fips != '-':
            df = df[df['state']==st_fips]
        df['yearspan'] = df['yearspan'].apply(lambda x: str(x))
        df['differflag'] = df.differflag.replace(1, True).fillna(False)
        df['trendbreak'] = df.trendbreak.replace(1, True).fillna(False)
        df = df.apply(lambda row: self.format_row(row), axis=1)
        df = self.get_locales(df)
        df['measure_id'] = df['measureid']
        df = df[['locale_id','measure_id','start_year','end_year','numerator','denominator','rawvalue','cilow','cihigh','chrreleaseyear','differflag','trendbreak']]
        df.chrreleaseyear = df.chrreleaseyear.astype("Int64")
        return df

    def load_health(self, st_fips):
        """
        Loads Flu vaccine data to database.
        Uses Pandas and SQLAlchemy. If the data is already in the database, it alerts the user.
        """
        urllib.request.urlretrieve('https://world-modelers.s3.amazonaws.com/data/CHR/CHR_trends_csv_2020.csv', 'CHR_trends_csv_2020.csv')
        urllib.request.urlretrieve('https://world-modelers.s3.amazonaws.com/data/CHR/CHR_measures.csv', 'CHR_measures.csv')
        health = self.process_health_file(st_fips)
        measures = pd.read_csv("CHR_measures.csv")
        try:
            measures.to_sql('measures', con=self.engine, schema=self.dbi.pg_schema_health, index=False, if_exists='append')
        except IntegrityError as e:
            assert isinstance(e.orig, UniqueViolation)
            print("Health measures metadata is already loaded in LocaleDB.")

        try:
            health.to_sql('health', con=self.engine, schema=self.dbi.pg_schema_health, index=False, if_exists='append')
            print(f"Loaded health data for {st_fips} successfully.")
        except IntegrityError as e:
            assert isinstance(e.orig, UniqueViolation)
            print(f"Health data for {st_fips} is already loaded in LocaleDB.")            

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_health}.health;')
            print(c.fetchone().n)


# ----------------------------------------------------------------------------------------------------------------------
class WeatherSchema(Schema):
    """The major parameters in this file are sequential climatic county monthly maximum, minimum and average temperature (deg. F. to 10ths) and precipitation (inches to 100ths). Period of record is 1895 through latest month available, updated monthly.

        Values from the most recent two calendar years will be updated on a monthly basis. Period of record updates will occur when the underlying data set undergoes a version change.

        METHODOLOGY:

        County values in nClimDiv were derived from area-weighted averages of grid-point estimates interpolated from station data. A nominal grid resolution of 5 km was used to ensure that all divisions had sufficient spatial sampling (only four small divisions had less than 100 points) and because the impact of elevation on precipitation is minimal below 5 km. Station data were gridded via climatologically aided interpolation to minimize biases from topographic and network variability.

        The Global Historical Climatology Network (GHCN) Daily dataset is the source of station data for nClimDiv. GHCN-Daily contains several major observing networks in North America, five of which are used here. The primary network is the National Weather Service (NWS) Cooperative Observing (COOP) program, which consists of stations operated by volunteers as well as by agencies such as the Federal Aviation Administration.

        Data is updated monthly.
    """

    def countdown(self, t):
        while t: 
            mins, secs = divmod(t, 60) 
            timer = '{:02d}:{:02d}'.format(mins, secs) 
            print("Retry in: " + timer + " seconds", end="\r") 
            time.sleep(1) 
            t -= 1

    def download_noaa(self, max_tries, min_delay, max_delay):
        i = 1
        while i <= max_tries:
            
            try:
                # Download (TO WRKDIR) 4 county weather files from NOAA ftp
                ftp = FTP('ftp.ncdc.noaa.gov') # ftp access to ncdc.noaa.gov
                ftp.login()                     # anonymous ftp login
                ftp.cwd('pub/data/cirs/climdiv') # change directory
      
                # Get all the files on the ftp page and Filter to only the 4 county files
                dirs = ftp.nlst() 
                description_files = [i for i in dirs if len(i.split('.'))>1]

                #Delete any partial downloads
                for file in description_files:
                    if os.path.exists(file):
                        os.remove(file)
                        print(f"Deleted {file}")
                
                files_to_download = []
                for file in description_files:

                    if "climdiv-pcpncy" in file or "climdiv-tmaxcy" in file or "climdiv-tmincy"in file or "climdiv-tmpccy" in file:
                        files_to_download.append(file)
                
                for file in files_to_download:  
                    if os.path.isfile(file):
                        print('Already downloaded file: '+ file)
                        continue
      
                    with open(file, 'wb') as fp:
                        print(f'Downloading: {file.split("/")[-1]}')
                        ftp.retrbinary('RETR ' + file, fp.write)
            
                i = 11  
                print("\n")        
                print(f"Complete. Files downloaded to: {os.getcwd()}") 
                
            except Exception as e:
                print(f'Exception: {e}')
                if i <= max_tries:
                    
                    sleep_time = rd.randint(min_delay, max_delay)
                    self.countdown(sleep_time)
                    
                    continue
                    
                else:
                    print(f'Exceeded {max_tries} max download attempts')
                    break
            i += 1  
        return files_to_download

    def year_filter(self,df, yr_filter_str):

        df['year'] = df.noaa_code.apply(lambda x: x[-4:])
        df = df[~df["year"].str.contains('|'.join(yr_filter_str))]
        df = df.reset_index(drop=True)
        
        del df['year']
        
        return df

    # readin NOAA data and apply year filter
    def read_filter_data(self, file):
        
        fn = file.split("/")[-1]
        print(f'Reading:    {fn}')

        names = ['noaa_code',1,2,3,4,5,6,7,8,9,10,11,12] 
        df = pd.read_csv(file, delim_whitespace=True, 
                         converters={'noaa_code': lambda x: str(x)},
                         engine='python',
                         names=names, 
                         header=None)

        # Filter by selected years:
        print(f"Filtering:  {fn}")
        df = self.year_filter(df, self.yr_filter_str)
        
        return df

    # pivot wx data from column to row
    def restack_df(self, df,fn):
        
        if fn == "01":
            wx = "precipitation"
        if fn == "02":
            wx = "Tavg"        
        if fn == "27":
            wx = "Tmax"        
        if fn == "28":
            wx = "Tmin"
        
        df = pd.DataFrame(df.set_index('noaa_code')\
                          .stack())\
                          .reset_index()\
                          .rename(columns={'level_1': 'month', 0: wx})
        return df

    # Build full census FIPS to add to df    
    def census_fip(self, row):
        county_fip = row.noaa_fips[-3:]
        census_fips = row.census_state_fips + county_fip
        
        return census_fips    
        
    # Remove "County" from county name
    def format_county(self, name):
        if "County" in name:
            name = name.replace("County", "").strip()
        return name

    # To avoid 4 columns of noaa_codes, replace the wx-type with "wx"
    def replace_it(self, x):
        temp = x[5:7]
        x = x.replace(temp,"wx")
        return x        

    def get_files(self):
        """
        Gets FIPS lookup data
        """
        print("Downloading FIPS lookups")
        urllib.request.urlretrieve('https://raw.githubusercontent.com/jataware/ASKE-weather/main/noaa_to_census/noaa_fips.txt', 'noaa_fips.txt')
        urllib.request.urlretrieve('https://raw.githubusercontent.com/jataware/ASKE-weather/main/noaa_to_census/noaa_states.txt', 'noaa_states.txt')
        urllib.request.urlretrieve('https://raw.githubusercontent.com/jataware/ASKE-weather/main/noaa_to_census/state_fips.txt', 'state_fips.txt')        
        print("Downloading NOAA data")
        files_to_download = self.download_noaa(5, 30, 60)


    def get_locales(self, df):
        locale_df = pd.read_sql("SELECT id, fips FROM main.locale where admin0='US'", self.engine)
        df = df.join(locale_df.set_index('fips'), how='inner', on='fips')
        df = df.rename(columns = {'id': 'locale_id'})
        del(df['fips'])
        return df


    def process_noaa(self, start_year, stop_year):
        # Build transform from noaa state fips to census fips
        # NOAA state-level FIPS from NOAA README
        noaa = f"{os.getcwd()}/noaa_states.txt"
        noaa_conv = pd.read_csv(noaa, sep=",", converters={'code_noaa': lambda x: str(x)},engine='python')

        # Census state-level FIPS 
        state_fips = f"{os.getcwd()}/state_fips.txt"
        census_conv = pd.read_csv(state_fips, sep="\t", converters={'code': lambda x: str(x)}, engine='python')

        # No need for full state name; will use abbreviations
        del census_conv["Name"]

        # NOAA county-level FIPS with name
        noaa_fn = f"{os.getcwd()}/noaa_fips.txt"
        noaa_fips= pd.read_csv(noaa_fn, sep="\t", converters={'noaa_fips': lambda x: str(x)},engine='python')

        # Build lists to map NOAA to census state codes
        fips_rs = pd.concat([noaa_conv, census_conv], axis=1)
        noaa_code = list(fips_rs["code_noaa"])
        noaa_state = list(fips_rs["state_noaa"])
        census_state = list(fips_rs["state"])
        census_code = list(fips_rs["code"])

        # build dict to map census state FIPS to NOAA state fips
        trans = {}
        for i in range(len(census_state)):
            state = census_state[i]
            fips = census_code[i]
            trans[state] = [fips]
            
        for temp_st in trans.keys():
            for i in range(len(noaa_state)):
                temp_noaa_st = noaa_state[i]
                
                if temp_st == temp_noaa_st:
                    trans[temp_st].append(noaa_code[i])  

        # Delete census keys that do not have data in the NOAA data            
        del_keys = []            
        for temp_st in trans.keys():            
            if len(trans[temp_st]) == 1:
                   del_keys.append(temp_st)
        [trans.pop(key) for key in del_keys]

        #remove state abbrev as key: noaa state fips = key
        transformer = {}

        for key in trans.keys():
            census = trans[key][0]
            noaa = trans[key][1]
            state_abbr = key
            transformer[noaa] = [census, state_abbr]


        # Filter to year range user requested:
        base_years = [i for i in range(1895, 2021)]
        user_years = [i for i in range(int(start_year), int(stop_year) +1)]
        yr_filter = set(base_years) ^ set(user_years)
        self.yr_filter_str = [str(i) for i in yr_filter]


        # Back-up if ftp site fails; must have these files already in the directory
        files_to_download=["climdiv-pcpncy-v1.0.0-20201104", 
                           "climdiv-tmaxcy-v1.0.0-20201104", 
                           "climdiv-tmincy-v1.0.0-20201104", 
                           "climdiv-tmpccy-v1.0.0-20201104"]

        starter = f"{os.getcwd()}/"
        files = [starter + file for file in files_to_download]

        # Read in and filter NOAA data
        df_list = []
        for file in files:
            
            df_list.append(self.read_filter_data(file))
            
        # restack wx data column-to-row 
        df_stack = []
        for df in df_list:

            fn = df.noaa_code.iloc[0][5:7]

            df_ = self.restack_df(df,fn)
            
            df_ = df_[~df_['noaa_code'].astype(str).str.startswith('50')]
            
            df_['noaa_fips'] = df_.noaa_code.apply(lambda x: x[:5])
            
            df_stack.append(df_)

        # Convert NOAA to Census FIPS
        transformer_df = pd.DataFrame.from_dict(transformer).transpose().rename(columns = {0:'census_state_fips', 1: 'state'})
        noaa_fips['county_name'] = noaa_fips['county_name'].apply(lambda x: self.format_county(x))

        df_aug = []
        for df in df_stack:
            
            df_ = df.join(noaa_fips.set_index('noaa_fips'), how='left', on='noaa_fips')
            df_['noaa_state_fips'] = df_.noaa_fips.apply(lambda x: x[:2])
            df_ = df_.join(transformer_df, how='left', on='noaa_state_fips')
            df_['census_county_fips'] = df_.apply(lambda row: self.census_fip(row), axis=1)
            
            df_aug.append(df_)

        df_join = []
        for df in df_aug:
            del df["census_state_fips"]
            del df["noaa_state_fips"]
            df.rename(columns = {'noaa_fips':'noaa_county_fips', 
                                 'census_county_fips': 'fips',
                                 'Tavg': 'tavg',
                                 'Tmax': 'tmax',
                                 'Tmin': 'tmin',
                                 'precipitation': 'precip',
                                 'county_name': 'county'},
                                 inplace = True) 
            
            df = df.replace(-99.90,np.NaN)
            df = df.replace(-9.99,np.NaN)
            
            df_join.append(df)

        result = pd.concat(df_join, axis=1)
        _, i = np.unique(result.columns, return_index=True)
        res = result.iloc[:, i]
        res['year'] = res['noaa_code'].apply(lambda x: int(x[-4:]))
        res = res[["year", "month", "fips", "precip", "tavg", "tmin", "tmax"]]
        res = self.get_locales(res)
        return res


    def load_weather(self, start_year, stop_year):
        """
        Loads Flu vaccine data to database.
        Uses Pandas and SQLAlchemy. If the data is already in the database, it alerts the user.
        """
        print(f"Loading NOAA data from {start_year} through {stop_year} (inclusive)")
        self.get_files()
        weather = self.process_noaa(start_year, stop_year)
        print(weather.head())
        
        try:
            weather.to_sql('weather', con=self.engine, schema=self.dbi.pg_schema_weather, index=False, if_exists='append')
            print(f"Loaded weather data for {start_year} through {stop_year} successfully.")
        except IntegrityError as e:
            assert isinstance(e.orig, UniqueViolation)
            print(f"Weather data for {start_year} through {stop_year} is already loaded in LocaleDB.")            

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_weather}.weather;')
            print(c.fetchone().n)            

# ----------------------------------------------------------------------------------------------------------------------
class MobilitySchema(Schema):
    """County mobility data is available here: https://data.bts.gov/api/views/w96p-f2qv/rows.csv?accessType=DOWNLOAD

    Data mirror: https://world-modelers.s3.amazonaws.com/data/Trips_by_Distance.csv 
    (will become stale)
    """


    def add_zero(self, x):
        # For FIPS sans leading zero
        if len(x)<5:
            return "0" + x
        else:
            return x   

    def get_locales(self, df):
        locale_df = pd.read_sql("SELECT id, fips FROM main.locale where admin0='US'", self.engine)
        df = df.join(locale_df.set_index('fips'), how='inner', on='fips')
        df = df.rename(columns = {'id': 'locale_id'})
        del(df['fips'])
        return df


    def stamper(self, x):
        temp = x.split("/")

        y = temp[0]
        m = temp[1]
        d = temp[2]
        
        return f'{y}-{m}-{d}'

    def process_mobility(self, state):
        # Read in data...new version of csv
        trips_fn = 'Trips_by_Distance.csv'
        #wrkdir = os.getcwd()

        df_full = pd.read_csv(trips_fn, sep=",", 
                              converters={'County FIPS': lambda x: self.add_zero(str(x))},engine='python')

        df_full = df_full[df_full["Level"] == "County"]

                # Add timestamp
        df = df_full.copy()
        df["Timestamp"] = df.Date.apply(lambda x: self.stamper(x))

        # Delete unneeded columns
        delete_me = ["Level", "Date", "State FIPS"]
        for me in delete_me:
            del df[me]
            
        # Rename column
        df.rename(columns={"State Postal Code": "State"}, inplace=True)

        # Filter by State
        if state != '-':
            df = df[df['State'] == state]

        # Drop "County" from County Name
        df["County Name"]=  df["County Name"].apply(lambda x: str(x).replace("County", "").strip())

        # Reorder columns
        new_cols = ['Timestamp', 'County Name', 'State', 'County FIPS','Population Staying at Home',
                'Population Not Staying at Home', 'Number of Trips',
                'Number of Trips <1', 'Number of Trips 1-3', 'Number of Trips 3-5',
                'Number of Trips 5-10', 'Number of Trips 10-25',
                'Number of Trips 25-50', 'Number of Trips 50-100',
                'Number of Trips 100-250', 'Number of Trips 250-500',
                'Number of Trips >=500']
        df = df[new_cols]

        # Delete rows without data
        df = df.dropna()

        # Convert people to integers
        cols = list(df.columns[4:])
        for col in cols:
            df[col]= df[col].astype(int) 

        renamed_cols = ['timestamp', 'fips','population_staying_at_home',
                    'population_not_staying_at_home', 'number_of_trips',
                    'number_of_trips_lt_1', 'number_of_trips_1_3', 'number_of_trips_3_5',
                    'number_of_trips_5_10', 'number_of_trips_10_25',
                    'number_of_trips_25_50', 'number_of_trips_50_100',
                    'number_of_trips_100_250', 'number_of_trips_250_500',
                    'number_of_trips_gte_500']       

        del(df['County Name'])
        del(df['State'])
        df.columns = renamed_cols
        df = self.get_locales(df)
        return df       

    def load_mobility(self, state):
        """
        Loads Flu vaccine data to database.
        Uses Pandas and SQLAlchemy. If the data is already in the database, it alerts the user.
        """
        print("\nDownloading mobility data...")
        download_url = 'https://data.bts.gov/api/views/w96p-f2qv/rows.csv?accessType=DOWNLOAD'
        urllib.request.urlretrieve(download_url, 'Trips_by_Distance.csv')
        print("...download complete!\nProcessing data...")
        mobility = self.process_mobility(state)
        print("...data processing complete!\nSample:\n")
        print(mobility.head())
        try:
            mobility.to_sql('mobility', con=self.engine, schema=self.dbi.pg_schema_mobility, index=False, if_exists='append')
        except IntegrityError as e:
            assert isinstance(e.orig, UniqueViolation)
            print("Mobility data is already loaded in LocaleDB.")

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_mobility}.mobility;')
            print(c.fetchone().n)

# ----------------------------------------------------------------------------------------------------------------------
class LocaleDB(object):
    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop, pg_schema_vax, pg_schema_health, pg_schema_weather, pg_schema_mobility, dpath_log, dpath_rt):
        self.dbi = DBI(pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop, pg_schema_vax, pg_schema_health, pg_schema_weather, pg_schema_mobility)
        self.fsi = FSI(dpath_log, dpath_rt)
        self.engine = create_engine(f'postgresql://{pg_usr}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}')

    def get_dis(self):
        return DiseaseSchema(self.dbi, self.fsi)

    def get_main(self):
        return MainSchema(self.dbi, self.fsi)

    def get_pop(self):
        return PopSchema(self.dbi, self.fsi)

    def get_vax(self):
        return VaxSchema(self.dbi, self.fsi, self.engine)

    def get_health(self):
        return HealthSchema(self.dbi, self.fsi, self.engine)        

    def get_weather(self):
        return WeatherSchema(self.dbi, self.fsi, self.engine)              

    def get_mobility(self):
        return MobilitySchema(self.dbi, self.fsi, self.engine)                        

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # print(sys.argv[1:])
    # print(len(sys.argv[1:]))
    # sys.exit(0)

    min_arg_length = 16

    if len(sys.argv[1:]) < min_arg_length:
        print(f'Incorrect number of arguments; at least ${min_arg_length} are required.')
        sys.exit(1)    

    if sys.argv[min_arg_length] == 'load-dis':
        req_argn(min_arg_length+1)
        LocaleDB(*sys.argv[1:min_arg_length]).get_dis().load_disease(sys.argv[min_arg_length+1])
    elif sys.argv[min_arg_length] == 'load-main':
        print(*sys.argv)
        LocaleDB(*sys.argv[1:min_arg_length]).get_main().load_locales()
    elif sys.argv[min_arg_length] == 'load-pop-state':
        req_argn(min_arg_length+1)
        LocaleDB(*sys.argv[1:min_arg_length]).get_pop().load_state(sys.argv[min_arg_length+1])
    elif sys.argv[min_arg_length] == 'load-vax':
        req_argn(min_arg_length)
        LocaleDB(*sys.argv[1:min_arg_length]).get_vax().load_vax()
    elif sys.argv[min_arg_length] == 'load-health':
        req_argn(min_arg_length+1)
        LocaleDB(*sys.argv[1:min_arg_length]).get_health().load_health(sys.argv[min_arg_length+1])        
    elif sys.argv[min_arg_length] == 'load-weather':
        req_argn(min_arg_length+2)
        LocaleDB(*sys.argv[1:min_arg_length]).get_weather().load_weather(sys.argv[min_arg_length+1], sys.argv[min_arg_length+2])                
    elif sys.argv[min_arg_length] == 'load-mobility':
        req_argn(min_arg_length+1)
        LocaleDB(*sys.argv[1:min_arg_length]).get_mobility().load_mobility(sys.argv[min_arg_length+1])
    else:
        print(f'Unknown command: {sys.argv[min_arg_length]}')
        sys.exit(1)