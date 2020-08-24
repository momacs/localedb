# -*- coding: utf-8 -*-
"""LocaleDB management routines (e.g., data import)."""

import io
import os
import psycopg2
import psycopg2.extras
import re
import sys

from collections import namedtuple
from pathlib     import Path


# ----------------------------------------------------------------------------------------------------------------------
def req_argn(n):
    """Requires the specified number of command line arguments passed to the script."""

    if len(sys.argv[1:]) != n:
        print(f'Incorrect number of arguments; expected {n} but {len(sys.argv[1:])} provided.')
        sys.exit(1)


# ----------------------------------------------------------------------------------------------------------------------
class DisDyn(object):
    pass


# ----------------------------------------------------------------------------------------------------------------------
class Evt(object):
    pass


# ----------------------------------------------------------------------------------------------------------------------
class Pop(object):
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

    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema, dpath_rt):
        self.pg_host   = pg_host
        self.pg_port   = pg_port
        self.pg_usr    = pg_usr
        self.pg_pwd    = pg_pwd
        self.pg_db     = pg_db
        self.pg_schema = pg_schema

        self.dpath_rt = Path(dpath_rt)  # runtime dir assumed to contain the source files uncompressed and ready for processing

        self.conn = psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_usr, password=self.pg_pwd, database=self.pg_db, cursor_factory=psycopg2.extras.NamedTupleCursor)

    def __del__(self):
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()
            self.conn = None

    def import_state(self, st_fips):
        """Imports a state to the database.

        The state ZIP file is expected to have been uncompressed to the self.dpath_rt directory.
        """

        with self.conn.cursor() as c:
            c.execute(self.__class__.SQL_CREATE_TEMP_TABLES.format(schema=self.pg_schema))
            c.execute('SET CONSTRAINTS ALL DEFERRED;')
            for ctf in self.__class__.COUNTY_TXT_FILES:
                self.import_county_txt_files(c, ctf, st_fips)
        self.conn.commit()

    def import_county_txt_files(self, c, county_txt_file, st_fips):
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

                c.copy_expert(county_txt_file.copy_sql.format(schema=self.pg_schema), f02)

                # Store the state FIPS code:
                c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tmp_{county_txt_file.tbl}' AND column_name = 'st_fips');")
                if bool(c.fetchone()[0]):
                    c.execute(f"UPDATE tmp_{county_txt_file.tbl} SET st_fips = '{st_fips}';")

                # Update the GEOM column and transform to the target srid:
                if county_txt_file.upd_coords_col:
                    c.execute(f"UPDATE tmp_{county_txt_file.tbl} SET coords = ST_GeomFromText('POINT(' || long || ' ' || lat || ')', 4326) WHERE lat != 0 AND long != 0;")
                    c.execute(f'UPDATE tmp_{county_txt_file.tbl} SET coords = ST_Transform(coords, 4269) WHERE lat != 0 AND long != 0;')

                # Populate the destination table:
                c.execute(f'INSERT INTO {self.pg_schema}.{county_txt_file.tbl} SELECT * FROM tmp_{county_txt_file.tbl} ON CONFLICT DO NOTHING;')
                c.execute(f'TRUNCATE tmp_{county_txt_file.tbl};')

    def test(self):
        with self.conn.cursor() as c:
            c.execute(f'SELECT COUNT(*) AS n FROM {self.pg_schema}.school;')
            print(c.fetchone().n)


# ----------------------------------------------------------------------------------------------------------------------
class LocaleDB(object):
    def __init__(self, pg_host, pg_port, pg_usr, pg_pwd, pg_db, pg_schema_geo, pg_schema_pop, dpath_rt):
        self.pg_host       = pg_host
        self.pg_port       = pg_port
        self.pg_usr        = pg_usr
        self.pg_pwd        = pg_pwd
        self.pg_db         = pg_db
        self.pg_schema_geo = pg_schema_geo
        self.pg_schema_pop = pg_schema_pop

        self.dpath_rt = dpath_rt

    def get_pop(self):
        return Pop(self.pg_host, self.pg_port, self.pg_usr, self.pg_pwd, self.pg_db, self.pg_schema_pop, self.dpath_rt)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # print(sys.argv[1:])
    # print(len(sys.argv[1:]))
    # sys.exit(0)

    if len(sys.argv[1:]) < 9:
        print(f'Incorrect number of arguments; at least nine are required.')
        sys.exit(1)

    if sys.argv[9] == 'import-pop-state':
        req_argn(10)
        LocaleDB(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]).get_pop().import_state(sys.argv[10])
    else:
        print(f'Unknown command: {sys.argv[9]}')
