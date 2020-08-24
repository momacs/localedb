# -*- coding: utf-8 -*-
"""Functionality built on top of LocaleDB data."""

import psycopg2
import psycopg2.extras

from pram.util import PgDB


__all__ = ['LocaleDB']


# ----------------------------------------------------------------------------------------------------------------------
class LocaleDB(object):
    def __init__(self, pg_host='localhost', pg_port='5433', pg_usr='postgres', pg_pwd='sa', pg_db='c19', pg_schema_geo='geo', pg_schema_pop='pop'):
        self.pg_host       = pg_host
        self.pg_port       = pg_port
        self.pg_usr        = pg_usr
        self.pg_pwd        = pg_pwd
        self.pg_db         = pg_db
        self.pg_schema_geo = pg_schema_geo
        self.pg_schema_pop = pg_schema_pop

        self.conn = self._get_new_conn()

    def _get_id(self, tbl, col='rowid', where=None):
        return self._get_num(conn, tbl, 'rowid', where)

    def _get_row_cnt(self, tbl, where=None):
        return self._get_num(tbl, 'COUNT(*)', where)

    def _get_new_conn(self, cursor_factory=psycopg2.extras.NamedTupleCursor):
        return psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_usr, password=self.pg_pwd, database=self.pg_db, cursor_factory=cursor_factory)

    def _get_num(self, tbl, col, where=None):
        where = '' if where is None else f' WHERE {where}'
        with self.conn.cursor() as c:
            c.execute(f'SELECT {col} FROM {tbl}{where}')
            row = c.fetchone()
            return row[0] if row else None

    def exec(self, qry):
        with self.conn.cursor() as c:
            c.execute(qry)

    def exec_get(self, qry):
        with self.conn.cursor() as c:
            c.execute(qry)
            return c.fetchall()

    def get_pop(self):
        return self._get_row_cnt('person_view')

    def ls_geo_co(self, st_fips):
        return self.exec_get(f"SELECT gid, statefp10, countyfp10, geoid10, name10, namelsad10 FROM geo.co WHERE statefp10 = '{st_fips}' ORDER BY geoid10;")

    def ls_geo_st(self):
        return self.exec_get('SELECT gid, statefp10, geoid10, stusps10, name10 FROM geo.st ORDER BY geoid10;')

    def set_pop_view_household(self, stcotrbg):
        self.exec(f"""
            DROP VIEW IF EXISTS person_view;
            CREATE OR REPLACE TEMP VIEW person_view AS
            SELECT p.*
            FROM pop.person AS p
            INNER JOIN pop.household AS h ON p.household_id = h.id
            WHERE h.stcotrbg LIKE '{stcotrbg}%';
        """)

    def set_pop_view_household_geo(self, stcotrbg, geo_tbl):
        self.exec(f"""
            DROP VIEW IF EXISTS person_view;
            CREATE OR REPLACE TEMP VIEW person_view AS
            SELECT p.*, g.gid AS household_geo_id
            FROM pop.person AS p
            INNER JOIN pop.household AS h ON p.household_id = h.id
            INNER JOIN geo.{geo_tbl} AS g ON ST_Contains(g.geom, h.coords)
            WHERE h.stcotrbg LIKE '{stcotrbg}%';
        """)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    db = LocaleDB()

    # print(db.ls_geo_st())
    print(db.ls_geo_co('02'))

    # db.set_pop_view_household('02')
    # print(db.get_pop())

    # db.set_pop_view_household('10')
    # print(db.get_pop())

    db.set_pop_view_household('02013')
    print(db.get_pop())
    print(db.exec_get('SELECT COUNT(*) FROM person_view p INNER JOIN pop.school s ON p.school_id = s.id'))

    db.set_pop_view_household('02016')
    print(db.get_pop())
    print(db.exec_get('SELECT COUNT(*) FROM person_view p INNER JOIN pop.school s ON p.school_id = s.id'))
