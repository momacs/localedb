# -*- coding: utf-8 -*-
"""Functionality built on top of LocaleDB data."""

import numpy as np
import psycopg2
import psycopg2.extras
import sys

from numpy import linalg


__all__ = ['LocaleDB']


# ----------------------------------------------------------------------------------------------------------------------
class UnknownLocaleError(Exception): pass
class UnknownDiseaseError: pass
class ObjectStateError(Exception): pass


# ----------------------------------------------------------------------------------------------------------------------
class Result(object):
    def __init__(self, res=None, ok=True, err_msg=None):
        self.ok = ok
        self.err_msg = err_msg
        self.res = res


# ----------------------------------------------------------------------------------------------------------------------
class LocaleDB(object):
    CURSOR_NAME_PREFIX = 'localedb-py'

    def __init__(self, pg_host='localhost', pg_port='5433', pg_usr='postgres', pg_pwd='sa', pg_db='localedb'):
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_usr  = pg_usr
        self.pg_pwd  = pg_pwd
        self.pg_db   = pg_db

        self.locale_id = None
        self.disease_id = None
        self.locale_fips = None  # not None for the US only (per the main.locale table)

        self.conn = self._get_new_conn()

        self.cursor_num = -1

    def _exec(self, qry, vars=None, do_get=True, itersize=2000):
        with self.conn.cursor() as c:
            if itersize > 0:
                c.itersize = itersize
            c.execute(qry, vars)
            if do_get:
                return c.fetchall()

    def _get_next_cursor_name(self):
        self.cursor_num += 1
        return f'{self.CURSOR_NAME_PREFIX}-{self.cursor_num}'

    def _get_id(self, tbl, col='rowid', where_sql=None, where_vars=None):
        return self._get_num(conn, tbl, 'rowid', where_sql, where_vars)

    def _get_row_cnt(self, tbl, where_sql=None, where_vars=None):
        return self._get_num(tbl, 'COUNT(*)', where_sql, where_vars)

    def _get_new_conn(self, cursor_factory=psycopg2.extras.NamedTupleCursor):
        return psycopg2.connect(host=self.pg_host, port=self.pg_port, user=self.pg_usr, password=self.pg_pwd, database=self.pg_db, cursor_factory=cursor_factory)

    def _get_num(self, tbl, col, where_sql=None, where_vars=None):
        where_sql = '' if where_sql is None else f' WHERE {where_sql}'
        with self.conn.cursor() as c:
            c.execute(f'SELECT {col} FROM {tbl}{where_sql};', where_vars)
            row = c.fetchone()
            return row[0] if row else None

    def _req_disease(self):
        if self.disease_id is None:
            raise ObjectStateError('No disease has been set')

    def _req_locale(self, do_req_us=False):
        if self.locale_id is None:
            raise ObjectStateError('No locale has been set')
        if do_req_us and self.locale_iso_num != 840:
            raise ObjectStateError('A U.S. locale is required')

    def _set_pop_view_household(self, fips):
        self._exec(f"""
            DROP VIEW IF EXISTS pop_person_view;
            CREATE OR REPLACE TEMP VIEW pop_person_view AS
            SELECT p.*
            FROM pop.person AS p
            INNER JOIN pop.household AS h ON p.household_id = h.id
            WHERE h.stcotrbg LIKE '{fips}%';
        """)

    def _set_pop_view_household_geo(self, fips, geo_tbl):
        return
        self._exec(f"""
            DROP VIEW IF EXISTS pop_person_view;
            CREATE OR REPLACE TEMP VIEW pop_person_view AS
            SELECT p.*, g.gid AS household_geo_id
            FROM pop.person AS p
            INNER JOIN pop.household AS h ON p.household_id = h.id
            INNER JOIN geo.{geo_tbl} AS g ON ST_Contains(g.geom, h.coords)
            WHERE h.stcotrbg LIKE '{fips}%';
        """)

    def get_dis_dyn_norm(self, conf, dead, do_inc_delta=False):
        self._req_disease() and self._req_locale()
        # res = self.get_dis_dyn_delta(conf, dead)
        # if not res.ok:
        #     return res
        # delta = res.res
        delta = self.get_dis_dyn_delta_by_day(conf, dead)

        # norm1 = np.sum(arr1 ** 2)
        # norm2 = np.sum(arr2 ** 2)
        # norm = np.sum((arr1 - arr2) ** 2)

        return {
            'conf': linalg.norm(delta['conf']),
            'dead': linalg.norm(delta['dead']),
            'delta': None if not do_inc_delta else delta
        }

    def _get_dis_dyn_comp_stats_x(self, x, vals, day_from=1, day_to=sys.maxsize, itersize=2000):
        Y_obs = np.array(self._get_dis_dyn_by_day_x(x, day_from, day_to, itersize)).flatten()
        Y_hat = np.array(vals)
        if Y_obs.size != Y_hat.size:
            raise ValueError(f'The sizes of the observed ({Y_obs.size}) and predicted ({Y_hat.size}) time series do not match.')

        # Corr:
        corr = np.corrcoef(Y_obs, Y_hat)[0,1]
        if np.isnan(corr):
            corr = 0.0

        # MAE:
        mae = np.absolute(Y_obs - Y_hat).mean()

        # RMSE:
        rmse = np.linalg.norm(Y_obs - Y_hat) / np.sqrt(len(Y_obs))

        # SRMSE:
        ybar = Y_obs.mean()
        srmse = rmse / ybar

        # R2:
        u = np.sum((Y_hat - Y_obs)**2)
        v = np.sum((Y_obs - ybar)**2)
        r2 = 1.0 - u / v

        return { 'corr': corr, 'mae': mae, 'rmse': rmse, 'srmse': srmse, 'r2': r2 }

    def get_dis_dyn_comp_stats(self, conf, dead, day_from=1, day_to=sys.maxsize, itersize=2000):
        return {
            'conf': self.get_dis_dyn_comp_stats_conf(conf, day_from, day_to, itersize),
            'dead': self.get_dis_dyn_comp_stats_dead(dead, day_from, day_to, itersize)
        }

    def get_dis_dyn_comp_stats_conf(self, vals, day_from=1, day_to=sys.maxsize, itersize=2000):
        return self._get_dis_dyn_comp_stats_x('n_conf', vals, day_from, day_to, itersize)

    def get_dis_dyn_comp_stats_dead(self, vals, day_from=1, day_to=sys.maxsize, itersize=2000):
        return self._get_dis_dyn_comp_stats_x('n_dead', vals, day_from, day_to, itersize)

    def _get_dis_dyn_by_day_x(self, x, day_from=1, day_to=sys.maxsize, itersize=2000):
        self._req_disease() and self._req_locale()
        if day_from > day_to:
            raise ValueError('Incorrect day range')
        res = {}
        return np.array(
            self._exec(
                f'SELECT {x} FROM dis.dyn WHERE disease_id = %s AND locale_id = %s AND day_i BETWEEN %s AND %s ORDER BY day_i;',
                [self.disease_id, self.locale_id, day_from, day_to],
                itersize
            )
        )

    def get_dis_dyn_by_day_conf(self, day_from=1, day_to=sys.maxsize, itersize=2000):
        return self._get_dis_dyn_by_day_x('n_conf', day_from, day_to, itersize)

    def get_dis_dyn_by_day_dead(self, day_from=1, day_to=sys.maxsize, itersize=2000):
        return self._get_dis_dyn_by_day_x('n_dead', day_from, day_to, itersize)

    def get_dis_dyn_by_day(self, do_get_conf=False, do_get_dead=False, day_from=1, day_to=sys.maxsize, itersize=2000):
        self._req_disease() and self._req_locale()
        if day_from > day_to:
            raise ValueError('Incorrect day range')
        res = {}
        if do_get_conf:
            res['conf'] = np.array(
                self._exec(
                    'SELECT n_conf FROM dis.dyn WHERE disease_id = %s AND locale_id = %s AND day_i BETWEEN %s AND %s ORDER BY day_i;',
                    [self.disease_id, self.locale_id, day_from, day_to],
                    itersize
                )
            )
        if do_get_dead:
            res['dead'] = np.array(
                self._exec(
                    'SELECT n_dead FROM dis.dyn WHERE disease_id = %s AND locale_id = %s AND day_i BETWEEN %s AND %s ORDER BY day_i;',
                    [self.disease_id, self.locale_id, day_from, day_to],
                    itersize
                )
            )
        return res

    def get_dis_dyn_delta_by_day(self, conf=None, dead=None, day_from=1, day_to=sys.maxsize, itersize=2000):
        self._req_disease() and self._req_locale()
        if day_from > day_to:
            raise ValueError('Incorrect day range')
        res = {}
        if conf:
            conf_obs = np.array(
                self._exec(
                    'SELECT n_conf FROM dis.dyn WHERE disease_id = %s AND locale_id = %s AND day_i BETWEEN %s AND %s ORDER BY day_i;',
                    [self.disease_id, self.locale_id, day_from, day_to],
                    itersize
                )
            )
            if len(conf_obs) != len(conf):
                raise ValueError('The sizes of the confirmed cases time series provided is incongruent with the observed one; the database may not contain enough data or the date range is incorrect.')
            res['conf'] = conf_obs - np.ndarray(conf)
        if dead:
            dead_obs = np.array(
                self._exec(
                    'SELECT n_dead FROM dis.dyn WHERE disease_id = %s AND locale_id = %s AND day_i BETWEEN %s AND %s ORDER BY day_i;',
                    [self.disease_id, self.locale_id, day_from, day_to],
                    itersize
                )
            )
            if len(dead_obs) != len(dead):
                raise ValueError('The sizes of the dead cases time series provided is incongruent with the observed one; the database may not contain enough data or the date range is incorrect.')
            res['dead'] = dead_obs - np.ndarray(dead)
        return res

    def get_locale_inf(self):
        pass
        # self._check_locale()
        # inf = self._exec(f'SELECT iso2, iso3 FROM main.locale WHERE id = ?;', [self.locale_id])[0]
        # return f'{inf.iso2} {inf.iso3}'

    def get_geo_counties(self, st_fips):
        return self._exec(f"SELECT gid, statefp10, countyfp10, geoid10, name10, namelsad10 FROM geo.co WHERE statefp10 = %s ORDER BY geoid10;", [st_fips])

    def get_geo_states(self):
        return self._exec('SELECT gid, statefp10, geoid10, stusps10, name10 FROM geo.st ORDER BY geoid10;')

    def get_pop_size(self):
        self._req_locale()
        return self._exec('SELECT pop FROM main.locale WHERE id = %s;', [self.locale_id])[0].pop

    def get_pop_size_synth(self):
        """Get the size of the U.S. synthetic population that is currently loaded into the database.  The U.S. only
        restriction stems from the fact that currently no other country is covered.  This method is most useful for
        states and counties because synthetic population data is loaded on a per state basis.  Consequently, unless all
        the states are loaded, the entire U.S. synthetic population size will be artifically low.

        Returns:
            int: -1 for non-US locale; non-negative integer for US locales.
        """

        if not self.is_locale_us():
            return -1

        # return self._get_row_cnt('pop_person_view')
        # return self._exec_get('WITH h AS (SELECT id FROM pop.household WHERE stcotrbg LIKE %s) SELECT COUNT(*) FROM pop.person p WHERE p.household_id IN (SELECT id FROM h);', [f'{self.locale_fips}%'])[0][0]
        # return self._exec_get('SELECT COUNT(*) FROM pop.person AS p INNER JOIN pop.household AS h ON p.household_id = h.id WHERE h.stcotrbg LIKE %s;', [f'{self.locale_fips}%'])[0][0]

        if self.locale_fips is None:  # entire US
            return self._exec('SELECT COUNT(*) FROM pop.person;')[0][0]
        elif len(self.locale_fips) == 2:  # US state
            return self._exec('SELECT COUNT(*) FROM pop.person p INNER JOIN pop.household h ON p.household_id = h.id INNER JOIN main.locale l ON h.st_id = l.id WHERE l.fips = %s;', [self.locale_fips])[0][0]
        elif len(self.locale_fips) == 5:  # US county
            return self._exec('SELECT COUNT(*) FROM pop.person p INNER JOIN pop.household h ON p.household_id = h.id INNER JOIN main.locale l ON h.co_id = l.id WHERE l.fips = %s;', [self.locale_fips])[0][0]
        else:
            raise ValueError('Incorrect FIPS code: {self.locale_fips}')

    def get_synth_pop(self, cols=['age'], limit=0, itersize=2000):
        self._req_locale(True)
        limit = f'LIMIT {limit}' if limit > 0 else ''
        if len(self.locale_fips) == 2:    # US state
            locale_id_col = 'st_id'
        elif len(self.locale_fips) == 5:  # US county
            locale_id_col = 'co_id'
        return self._exec(
            f'''
            SELECT {",".join(cols)}
            FROM pop.person p
            INNER JOIN pop.household h ON p.household_id = h.id
            INNER JOIN main.locale l ON h.{locale_id_col} = l.id
            WHERE l.id = %s
            ORDER BY p.id {limit};
            ''', [self.locale_id], itersize
        )

    def is_locale_us(self):
        return self.locale_id is not None and self.locale_iso_num == 840

    def set_disease(self, name):
        self.disease_id = self._get_num('dis.disease', 'id', 'name = %s', [name])
        if self.disease_id is None:
            raise UnknownDiseaseError(f'Disease not found: {name}')
        return self

    def set_locale_by_name(self, admin0, admin1=None, admin2=None):
        with self.conn.cursor() as c:
            c.execute('SELECT id, iso_num, fips FROM main.locale WHERE admin0 = %s AND admin1 IS NOT DISTINCT FROM %s AND admin2 IS NOT DISTINCT FROM %s;', [admin0, admin1, admin2])
            if c.rowcount == 0:
                raise UnknownLocaleError(f'No locale found with the following name: {admin0}, {admin1}, {admin2}')
            r = c.fetchone()
            self.locale_id      = r.id
            self.locale_iso_num = r.iso_num
            self.locale_fips    = r.fips

        # if self.locale_fips is not None:
        #     self._set_pop_view_household(self.locale_fips)
        #     self._set_pop_view_household_geo(self.locale_fips, 'st')

        return self

    def set_locale_by_us_fips(self, fips=None):
        with self.conn.cursor() as c:
            c.execute('SELECT id FROM main.locale WHERE iso_num = %s AND fips IS NOT DISTINCT FROM %s;', [840, fips])
            if c.rowcount == 0:
                raise UnknownLocaleError(f'No U.S. locale found with the following FIPS code: {fips}')
            self.locale_id = c.fetchone().id

        self.locale_iso_num = 840
        self.locale_fips = fips
        # self._set_pop_view_household(fips)
        # self._set_pop_view_household_geo(fips, 'st')

        return self


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    import time

    def disp_locale_inf(db):
        t0 = time.perf_counter()
        print(f'id: {db.locale_id}    iso_num: {db.locale_iso_num}    fips: {db.locale_fips}    pop: {db.get_pop_size()}    pop-synth: {db.get_pop_size_synth()}    ({time.perf_counter() - t0:.0f} s)', flush=True)

    def disp_locale_dis_dyn_by_day(db):
        conf = db.get_dis_dyn_by_day_conf()
        print(f"{db.locale_id}: n={conf.size}; {conf.flatten().tolist()[:48]}")

    def disp_synth_pop(db):
        c = db.get_synth_pop(['sex', 'age', 'WIDTH_BUCKET(age::INTEGER,ARRAY[18,60]) AS age_grp', 'income', 'CASE WHEN school_id IS NULL THEN 0 ELSE 1 END AS is_student', 'CASE WHEN workplace_id IS NULL THEN 0 ELSE 1 END is_worker'], limit=4)
        print(np.array(c).tolist())

    db = LocaleDB()
    db.set_disease('COVID-19')

    # Test basic population and synthetic population queries:
    db.set_locale_by_name('China')                            ; disp_locale_inf(db)
    db.set_locale_by_name('Italy')                            ; disp_locale_inf(db)
    db.set_locale_by_name('US')                               ; disp_locale_inf(db)

    db.set_locale_by_name('US', 'Alaska')                     ; disp_locale_inf(db)
    db.set_locale_by_us_fips('02')                            ; disp_locale_inf(db)
    db.set_locale_by_name('US', 'Alaska', 'Anchorage')        ; disp_locale_inf(db)
    db.set_locale_by_us_fips('02020')                         ; disp_locale_inf(db)

    db.set_locale_by_name('US', 'Pennsylvania')               ; disp_locale_inf(db)
    db.set_locale_by_us_fips('42')                            ; disp_locale_inf(db)
    db.set_locale_by_name('US', 'Pennsylvania', 'Allegheny')  ; disp_locale_inf(db)
    db.set_locale_by_us_fips('42003')                         ; disp_locale_inf(db)

    # Test disease dynamics queries:
    db.set_locale_by_name('China')                            ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('Italy')                            ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('US')                               ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('US', 'Alaska')                     ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('US', 'Alaska', 'Anchorage')        ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('US', 'Pennsylvania')               ; disp_locale_dis_dyn_by_day(db)
    db.set_locale_by_name('US', 'Pennsylvania', 'Allegheny')  ; disp_locale_dis_dyn_by_day(db)

    # Test disease dynamics comparison:
    db.set_locale_by_name('US')
    print(db.get_dis_dyn_comp_stats_conf([1, 1, 2, 2, 6], day_to=5))

    # Test synthetic population retrieval queries:
    db.set_locale_by_name('US', 'Pennsylvania')               ; disp_synth_pop(db)
    db.set_locale_by_name('US', 'Pennsylvania', 'Allegheny')  ; disp_synth_pop(db)
    db.set_locale_by_name('US', 'Pennsylvania', 'Adams')      ; disp_synth_pop(db)
