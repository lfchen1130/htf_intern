# encoding=utf-8
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import logging
import datetime
from dateutil.relativedelta import relativedelta
import urllib3
from bs4 import BeautifulSoup

db_conf = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'x',
         'db': 'ashare_207', 'charset': 'utf8'}

class DBUtil(object):

    def __init__(self, **kwargs):
        self.conn = pymysql.connect(host=kwargs['host'], user=kwargs['user'],
                passwd=kwargs['passwd'], db=kwargs['db'], charset=kwargs['charset'], port=int(kwargs['port']))

    def get_conn(self):
        return self.conn;

    def close(self):
        self.get_conn().close()

    def execute(self, sql, params=None):
        self.get_conn().begin()
        cur = self.get_conn().cursor()
        row = cur.execute(sql, params)
        cur.close()
        self.get_conn().commit()
        return row

    def executetrans(self, sqls, params=None):
        self.get_conn().begin()
        cur = self.get_conn().cursor()
        for i in range(len(sqls)):
            row = cur.executemany(sqls[i], params[i])
        cur.close()
        self.get_conn().commit()
        return row

    def executemany(self, sql, params=None):
        self.get_conn().begin()
        cur = self.get_conn().cursor()
        row = cur.executemany(sql, params)
        cur.close()
        self.get_conn().commit()
        return row

    def query(self, sql, params=None):
        cur = self.get_conn().cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return rows

    def querylist(self, sql, params=None):
        cur = self.get_conn().cursor()
        cur.execute(sql, params)
        cur.close()
        rows = cur.fetchall()
        lst = []
        for r in rows:
            lst.append(r[0])
        return lst

    def querydict(self, sql, params=None):
        cur = self.get_conn().cursor()
        cur.execute(sql, params)
        cur.close()
        rows = cur.fetchall()
        dic = {}
        for r in rows:
            dic[r[0]] = r[1]
        return dic
    
    def queryfordict(self, sql, params=None):
        cur = self.get_conn().cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql, params)
        cur.close()
        rows = cur.fetchall()
        return rows

    def queryone(self, sql, params=None):
        cur = self.get_conn().cursor()
        cur.execute(sql, params)
        cur.close()
        return cur.fetchone()

    def querydataframe(self, sql, params=None, index_col=None):
        df = pd.read_sql(sql, self.get_conn(), params=params, index_col=index_col)
        return df


def createDB(conf):
    db = DBUtil(**conf)
    return db


def createDBengine(conf):
    engine = create_engine('mysql://' + conf['user'] + ':' + conf['passwd'] + '@' 
            +conf['host'] + ':' + conf['port'] + '/' + conf['db'] + '?charset=' + conf['charset'])
    return engine

def get_date(date=None, offset=0, sep='-'):
    reg = '%Y' + sep + '%m' + sep + '%d'
    if date == None:
        d = datetime.date.today()
    else : 
        d = datetime.datetime.strptime(date, reg)
    if offset != 0:
        d = d + relativedelta(days=offset)
    return datetime.datetime.strftime(d, reg)

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(funcName)s - %(message)s')
log = logging.getLogger()
