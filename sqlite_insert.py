"""
SEP 26, 2019
Time Series Analysis HW # 1
Jiayu Wang, Yiting Wang, Xin Zou
"""

import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - [%(filename)s | func %(funcName)s | line %(lineno)s] [%(levelname)s] : %(''message)s')
import os
import copy
import sqlite3


class CreateDatabase(object):
    def __init__(self):
        self.DB_DIR = '/Users/yiting/Desktop/db/'
        self.db_name = 'climate_eff.db'
        self.table_name = 'climate_monthly'
        self.columns = ['date', 'region', 'latitude', 'longitude', 'precipitation', 'max_temp', 'min_temp', 'wind']
        self.types = ['TEXT', 'TEXT', 'NUMBER', ' NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER']

    def setup_db(self):
        assert self.DB_DIR == '/Users/yiting/Desktop/db/'
        files = os.listdir('/Users/yiting/Desktop/')
        if 'db' not in files:
            os.mkdir(self.DB_DIR)
            logging.info('created directory: %s' % self.DB_DIR)
        files = os.listdir(self.DB_DIR)
        if self.db_name not in files:
            logging.info('creating db: %s' % self.db_name)
            conn = sqlite3.connect(self.DB_DIR + self.db_name, check_same_thread=False, timeout=3000)
            conn.close()

    def create_table(self):
        head = ', '.join(['%s %s' % (c_t[0], c_t[1]) for c_t in zip(self.columns, self.types)])
        db = sqlite3.connect(self.DB_DIR + self.db_name, check_same_thread=False, timeout=3000)

        c = db.cursor()

        sql_stmt = "CREATE TABLE %s (%s)" % (self.table_name, head)
        logging.info(sql_stmt)
        c.execute(sql_stmt)

        db.commit()
        db.close()

    def post(self, records):
        # logging.info('posting: %d lines' % len(records))
        db = sqlite3.connect(self.DB_DIR + self.db_name, check_same_thread=False, timeout=3000)
        cursor = db.cursor()
        for r in records:
            self._insert(r, cursor)
        db.commit()
        db.close()
        # logging.info('posted: %d lines' % len(records))

    def query(self, sql_stmt, columns=None):
        logging.info(sql_stmt)
        db = sqlite3.connect(self.DB_DIR + self.db_name, check_same_thread=False, timeout=3000)
        cursor = db.cursor()
        tmp = cursor.execute(sql_stmt)
        result = tmp.fetchall()
        db.close()
        logging.info('found: %d lines' % len(result))
        if columns:
            return [dict(list(zip(columns, x))) for x in result]
        else:
            return result

    def _insert(self, record, cursor):
        columns = list(record.keys())
        values = []
        for v in record.values():
            if type(v) == str:
                values += ["'%s'" % v]
            else:
                values += ['%s' % v]
        sql_stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (self.table_name, ','.join(columns), ','.join(values))
        cursor.execute(sql_stmt)

    def parser(self, file_name, folder, x):
        r = copy.deepcopy(x)
        data, latitude, longitude = file_name.split('_')
        r['longitude'] = longitude
        r['latitude'] = latitude
        r['region'] = folder
        r['year'] = '%04d-%02d' % (int(r['year']), int(r['month']))
        del r['year']
        del r['month']
        return r


def main(env="local"):
    if env == "local":
        location = r'/Users/yiting/dropbox/BA_SCU/Time_Series/climate_database'
    elif env == "server":
        location = ''
    else:
        assert False, "unknown environment: %s" % env

    c = CreateDatabase()
    c.setup_db()
    c.create_table()

    folders = os.listdir(location)
    folders = list(filter(lambda f: '.DS_Store' not in f, folders))
    folder_no = 0
    for folder in folders:
        files = os.listdir(location + '/' + folder)
        folder_no += 1
        for file in files:
            file_content = open((location + '/' + folder + '/' + file))
            records = []
            for line in file_content:
                line = line.split()
                header = ['year', 'month', 'precipitation', 'max_temp', 'min_temp', 'wind']
                x = dict(zip(header, line))
                records.append(c.parser(file, folder, x))
            c.post(records)
            file_content.close()
    logging.info('done')
    logging.info('folder number is %d' % folder_no)


if __name__ == "__main__":
    main()
