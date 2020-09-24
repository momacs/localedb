# -*- coding: utf-8 -*-
"""LocaleDB management routines."""

import csv
import io
import math
import os
import psycopg2
import psycopg2.extras
import re
import sys
import time
import urllib.request

from abc         import ABC
from collections import namedtuple
from pathlib     import Path


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

    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop, cursor_factory=psycopg2.extras.NamedTupleCursor):
        self.pg_host        = pg_host
        self.pg_port        = pg_port
        self.pg_usr         = pg_usr
        self.pg_pwd         = pg_pwd
        self.pg_db          = pg_db
        self.pg_schema_dis  = pg_schema_dis
        self.pg_schema_geo  = pg_schema_geo
        self.pg_schema_main = pg_schema_main
        self.pg_schema_pop  = pg_schema_pop

        self.conn = psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_usr, password=self.pg_pwd, database=self.pg_db, cursor_factory=cursor_factory)

    def __del__(self):
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()
            self.conn = None

    def vacuum(self, tbl=None, do_full=False):
        self.conn.autocommit = True
        c = self.conn.cursor()
        c.execute(f'VACUUM (FULL, ANALYZE) {tbl or ""};' if do_full else f'VACUUM ANALYZE {tbl or ""};')
        c.close()
        self.conn.autocommit = False


# ----------------------------------------------------------------------------------------------------------------------
class Schema(ABC):
    """Database schema manager.

    Manages one type of data type.  Data type are compartmentalized into PostgreSQL's schemas.
    """

    def __init__(self, dbi, dpath_rt):
        self.dbi = dbi
        self.dpath_rt = Path(dpath_rt)  # runtime dir assumed to contain the source files uncompressed and ready for processing


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

    URL_DYN_C19_DEAD_GLOB = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    URL_DYN_C19_DEAD_US   = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
    URL_DYN_C19_CONF_GLOB = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    URL_DYN_C19_CONF_US   = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
    URL_DYN_C19_REC_GLOB  = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
    URL_DYN_C19_REC_US    = None  # dataset not available

    URL_NPI_C19_KEYSTONE = 'https://raw.githubusercontent.com/Keystone-Strategy/covid19-intervention-data/master/complete_npis_inherited_policies.csv'

    def load_disease(self, disease):
        {
            'c19'  : self.load_c19,
            'h1n1' : self.load_h1n1
        }.get(disease, lambda: print(f'Unknown disease: {disease}'))()

    def load_c19(self):
        disease_id = None
        with self.dbi.conn.cursor() as c:
            c.execute(f"INSERT INTO {self.dbi.pg_schema_dis}.disease (name) VALUES ('c19') ON CONFLICT DO NOTHING;")
            c.execute(f'SELECT id FROM {self.dbi.pg_schema_dis}.disease WHERE name = %s;', ['c19'])
            disease_id = c.fetchone()[0]
        self.dbi.conn.commit()

        self.load_c19_dyn(disease_id)
        self.load_c19_npi(disease_id)

    def load_c19_dyn(self, disease_id):
        print(f'Disease dynamics', flush=True)

        with self.dbi.conn.cursor() as c:
            c.execute(f'CREATE TEMPORARY TABLE dyn_load (LIKE {self.dbi.pg_schema_dis}.dyn INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);')

            self.load_c19_dyn_ds(c, disease_id, self.URL_DYN_C19_CONF_GLOB, 'n_conf', 'confirmed', True,  date_col_idx_0=4)
            self.load_c19_dyn_ds(c, disease_id, self.URL_DYN_C19_DEAD_GLOB, 'n_dead', 'deaths',    True,  date_col_idx_0=4)
            self.load_c19_dyn_ds(c, disease_id, self.URL_DYN_C19_REC_GLOB,  'n_rec',  'recovered', True,  date_col_idx_0=4)

            self.load_c19_dyn_ds(c, disease_id, self.URL_DYN_C19_CONF_US,   'n_conf', 'confirmed', False, date_col_idx_0=12)
            self.load_c19_dyn_ds(c, disease_id, self.URL_DYN_C19_DEAD_US,   'n_dead', 'deaths',    False, date_col_idx_0=12)

            print(f'    Consolidating...', end='', flush=True)
            t0 = time.perf_counter()
            c.execute(f'DELETE FROM {self.dbi.pg_schema_dis}.dyn WHERE disease_id = %s;', [disease_id])
            c.execute(f'INSERT INTO {self.dbi.pg_schema_dis}.dyn SELECT * FROM dyn_load;')
        self.dbi.conn.commit()
        self.dbi.vacuum(f'{self.dbi.pg_schema_dis}.dyn')
        print(f' done ({time.perf_counter() - t0:.0f} s)', flush=True)

    def load_c19_dyn_ds(self, c, disease_id, url, col, col_human, is_glob, date_col_idx_0, page_size=1024):
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

    def load_c19_npi(self, disease_id):
        print(f'Non-pharmaceutical interventions', flush=True)

        self.load_c19_npi_keystone(disease_id)

    def load_c19_npi_keystone(self, disease_id):
        print(f'    Loading Keystone...', end='', flush=True)
        t0 = time.perf_counter()

        # (1) Extract:
        res = urllib.request.urlopen(self.URL_NPI_C19_KEYSTONE)
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
        CountyTxtFile('schools.txt',    'school',    (False, re.compile(r'^\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')),           f"COPY tmp_school    (id, stco, lat, long)                                                     FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('hospitals.txt',  'hospital',  (True,  re.compile(r'^\d+\t\d+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_hospital  (id, worker_cnt, physician_cnt, bed_cnt, lat, long)                       FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('households.txt', 'household', (False, re.compile(r'^\d+\t\d+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_household (id, stcotrbg, race_id, income, lat, long)                                FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('gq.txt',         'gq',        (False, re.compile(r'^\d+\t\w+\t\d+\t\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')), f"COPY tmp_gq        (id, type, stcotrbg, person_cnt, lat, long)                               FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('workplaces.txt', 'workplace', (False, re.compile(r'^\d+\t-?[0-9]+\.[0-9]+\t-?[0-9]+\.[0-9]+$')),                f"COPY tmp_workplace (id, lat, long)                                                           FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", True),
        CountyTxtFile('people.txt',     'person',    (False, re.compile(r'^\d+\t\d+\t\d+\t[FM]\t\d+\t\d+\t(?:\d+|X)\t(?:\d+|X)$')),    f"COPY tmp_person    (id, household_id, age, sex, race_id, relate_id, school_id, workplace_id) FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", False),
        CountyTxtFile('gq_people.txt',  'gq_person', (False, re.compile(r'^\d+\t\d+\t\d+\t[FM]$')),                                    f"COPY tmp_gq_person (id, gq_id, age, sex)                                                     FROM stdin WITH CSV DELIMITER AS '\t' NULL AS '{NA}';", False)
    ]

    def load_state(self, st_fips):
        """Loads a state to the database.

        The state ZIP file is expected to have been uncompressed to the self.dpath_rt directory.
        """

        with self.dbi.conn.cursor() as c:
            c.execute(self.__class__.SQL_CREATE_TEMP_TABLES.format(schema=self.dbi.pg_schema_pop))
            c.execute('SET CONSTRAINTS ALL DEFERRED;')
            for ctf in self.__class__.COUNTY_TXT_FILES:
                self.load_county_txt_files(c, ctf, st_fips)
        self.dbi.conn.commit()

    def load_county_txt_files(self, c, county_txt_file, st_fips):
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

        for path_file in self.dpath_rt.rglob(county_txt_file.fname):
            if os.path.getsize(path_file) == 0:
                continue

            with open(path_file, 'r') as f01, io.StringIO() as f02:
                next(f01)

                if county_txt_file.ln_re[0]:
                    for ln in f01:
                        if county_txt_file.ln_re[1].match(ln):
                            f02.write(ln)
                        elif self.rep:
                            self.rep.write(ln)
                    f02.seek(0)
                else:
                    f02 = f01

                c.copy_expert(county_txt_file.copy_sql.format(schema=self.dbi.pg_schema_pop), f02)

                # Store the state FIPS code:
                c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{county_txt_file.tbl}' AND column_name = 'st_fips');")
                if bool(c.fetchone()[0]):
                    c.execute(f"UPDATE tmp_{county_txt_file.tbl} SET st_fips = '{st_fips}';")

                # Update the GEOM column and transform to the target srid:
                if county_txt_file.upd_coords_col:
                    c.execute(f"UPDATE tmp_{county_txt_file.tbl} SET coords = ST_GeomFromText('POINT(' || long || ' ' || lat || ')', 4326) WHERE lat != 0 AND long != 0;")
                    c.execute(f'UPDATE tmp_{county_txt_file.tbl} SET coords = ST_Transform(coords, 4269) WHERE lat != 0 AND long != 0;')

                # Populate the destination table:
                c.execute(f'INSERT INTO {self.dbi.pg_schema_pop}.{county_txt_file.tbl} SELECT * FROM tmp_{county_txt_file.tbl} ON CONFLICT DO NOTHING;')
                c.execute(f'TRUNCATE tmp_{county_txt_file.tbl};')

    def test(self):
        with self.conn.dbi.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.dbi.pg_schema_pop}.school;')
            print(c.fetchone().n)


# ----------------------------------------------------------------------------------------------------------------------
class LocaleDB(object):
    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop, dpath_rt):
        self.dbi = DBI(pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_dis, pg_schema_geo, pg_schema_main, pg_schema_pop)
        self.dpath_rt = dpath_rt

    def get_dis(self):
        return DiseaseSchema(self.dbi, self.dpath_rt)

    def get_main(self):
        return MainSchema(self.dbi, self.dpath_rt)

    def get_pop(self):
        return PopSchema(self.dbi, self.dpath_rt)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # print(sys.argv[1:])
    # print(len(sys.argv[1:]))
    # sys.exit(0)

    if len(sys.argv[1:]) < 11:
        print(f'Incorrect number of arguments; at least 11 are required.')
        sys.exit(1)

    if sys.argv[11] == 'load-dis':
        req_argn(12)
        LocaleDB(*sys.argv[1:11]).get_dis().load_disease(sys.argv[12])
    elif sys.argv[11] == 'load-main':
        LocaleDB(*sys.argv[1:11]).get_main().load_locales()
    elif sys.argv[11] == 'load-pop-state':
        req_argn(12)
        LocaleDB(*sys.argv[1:11]).get_pop().load_state(sys.argv[12])
    else:
        print(f'Unknown command: {sys.argv[11]}')
        sys.exit(1)
