import re
import sys
import pymysql
import datetime
import pandas as pd
import rqdatac as rq
from rqdatac import *
import time 


#class a database 
class Db():
    def __init__(self,host,user,password,database,port):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__port = port
        self.__conn = pymysql.connect(self.__host,self.__user,self.__password,self.__database,port = self.__port,charset='utf8mb4')
        self.__cursor = self.__conn.cursor()
    
    #private 
    def __execute_read(self,sql):
        # try:
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        info = cursor.fetchall()
        cursor.close()
        return info
        # except:
        #     print("can't read the table")
    
    def get_symbol(self,table_name,index_name):
        read_sql = "select distinct symbol from %s where index_symbol= '%s'"%(table_name,index_name)
        return self.__execute_read(read_sql)


    def get_table_name(self,db_):
        sql = 'show tables from %s'%(db_)
        return self.__execute_read(sql)
    
    def get_max_id(self,table_name):
        sql = f"""
        select  max(id) from {table_name}
        """
        return self.__execute_read(sql)

    def update_status(self,table_name):
        sql1 =f"""
        update {table_name} set `status`=0 WHERE volume>0;
        """ 
        sql2 = f"""
        update {table_name} set `status`=1 WHERE volume=0;
        """
        sql3 = f"""
        update {table_name} set `status`=2 WHERE volume IS NULL;
        """
        cursor = self.__conn.cursor()
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.execute(sql3)
        self.__conn.commit()

    def get_cols(self,table_name):
        return [s[0] for s in self.__execute_read(f"show columns from {table_name};")]
    #check  the newest date of the symbol in the table         
    def check_date(self,name,symbol):
        sql_search = """
        SELECT max(date) FROM %s WHERE symbol ='%s';
        """%(name,symbol)
        return self.__execute_read(sql_search)
    
    def check_financial_date(self,name,symbol):
        sql_search = """
        SELECT max(report_date) FROM %s WHERE symbol ='%s';
        """%(name,symbol)
        return self.__execute_read(sql_search)

    def check_rate_date(self,name,symbol):
        sql_search = """
        SELECT max(date) FROM %s WHERE currency ='%s';
        """%(name,symbol)
        return self.__execute_read(sql_search)


    def check_forecast_date(self,name,symbol,forecast_year):
        sql_search = """
        SELECT max(date) FROM %s WHERE symbol ='%s' and forecast_year = '%s';
        """%(name,symbol,forecast_year)
        return self.__execute_read(sql_search)

    def check_index_date(self,name,symbol):
        sql_search = """
        SELECT max(date) FROM %s WHERE index_symbol ='%s';
        """%(name,symbol)
        return self.__execute_read(sql_search)
    #insert data 
    def  insert_tick(self,name,df):
        tp1 = df.values
        sql_data = []
        for i in range(0,len(df.index)):
            tp = []
            for j in range(0,len(df.columns)):
                tp.append(tp1[i][j])
            sql_data.append(tp)
        sql_insert1 = 'insert into %s'%(name)
        # sql_insert2 = """(date,timestamp,symbol,open,last,high,low,prev_settlement,prev_close,volume,open_interest,total_turnover,limit_up,
        # limit_down,a1,a2,a3,a4,a5,b1,b2,b3,b4,b5,a1_v,
        # a2_v,a3_v,a4_v,a5_v,b1_v,b2_v,b3_v,b4_v,b5_v,change_rate) 
        # values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """
        sql_insert2 = """('date','timestamp','symbol','last','prev_settlement','prev_close','volume','open_interest','limit_up','limit_down','a1','a1_v','b1','b1_v') 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        sql_insert = sql_insert1 + sql_insert2

        # try:
        self.__cursor.executemany(sql_insert,sql_data)
        self.__conn.commit()
        # except:
        #     self.__conn.rollback()

    def  insert_description(self,db_name_,df):
        name = db_name_ +"_description"
        tp1 = df.values
        sql_data = []
        for i in range(0,len(df.index)):
            tp = []
            for j in range(0,len(df.columns)):
                tp.append(tp1[i][j])
            sql_data.append(tp)
        sql_insert1 = 'insert into %s'%(name)
        sql_insert2 = """(symbol,order_book_id,listed_date,maturity_date) 
        values(%s,%s,%s,%s)
        """
        sql_insert = sql_insert1 + sql_insert2

        # try:
        self.__cursor.executemany(sql_insert,sql_data)
        self.__conn.commit()
        # except:
        #     self.__conn.rollback()

    def  update_description(self,db_name_,dt_ ,table_):
        name = db_name_ +"_description"
        update_sql = "UPDATE {} SET download = '{}',update_time='{}' where order_book_id = '{}'".format(str(name),'y',str(dt_),str(table_))

        try:
            self.__cursor.execute(update_sql)
            self.__conn.commit()
        except:
            self.__conn.rollback()
    #close the database 
    def close_database(self):
        self.__conn.close()
#-------------create databse and table---------
class CreateDb():
    def __init__(self,host,user,password,DATABASES_INFO):
        self.__db_info = DATABASES_INFO
        self.__conn = pymysql.connect(host,user,password)
        self.__cursor = self.__conn.cursor()

    @staticmethod
    def abs_null(tb_info, num):
        if tb_info['columns'][num][2] == 0:
            return ''
        else:
            return 'NOT NULL'

    def create_database(self):
        self.__cursor.execute('show databases;')
        tables_tup = self.__cursor.fetchall()
        for db_info in self.__db_info:
            print(f"start create {db_info['db_name']}......")
            if f"('{db_info['db_name']}',)" not in str(tables_tup):
                try:
                    self.__cursor.execute(f'CREATE DATABASE {db_info["db_name"]} character set utf8mb4;')
                except Exception as e:
                    print(f'error:{e}')
                    self.__conn.rollback()
            else:
                print('database is already created')
            self.__create_table(db_info)
            print(f'{db_info["db_name"]} created successfully!')

    def __create_table(self, db_info):
        self.__cursor.execute(f'use {db_info["db_name"]};')
        self.__cursor.execute('SHOW tables;')
        table_list_info = self.__cursor.fetchall()
        table_list_info = [i[0] for i in table_list_info]
        for tb_info in db_info['tb_info']:
            if tb_info['tb_name'].lower() not in str(table_list_info):
                my_table = f'CREATE TABLE {tb_info["tb_name"]}(' \
                    f'date date NOT NULL,' \
                    f'timestamp varchar(255) NOT NULL,' \
                    f'symbol varchar(255) NOT NULL,' \
                    f'PRIMARY KEY (date,timestamp,symbol)' \
                    f')CHARSET="utf8mb4"'
                try:
                    self.__cursor.execute(my_table)
                except Exception as e:
                    print(f'error:{e}')
                    self.__conn.rollback()
            else:
                print(f"'{tb_info['tb_name']}' is already created")
            self.__add_column(db_info, tb_info)


    def __add_column(self, db_info, tb_info):
        self.__cursor.execute(f'SHOW COLUMNS FROM {tb_info["tb_name"]};')
        column_list = self.__cursor.fetchall()
        for i in range(len(tb_info['columns'])):
            if f"('{tb_info['columns'][i][0]}'" not in str(column_list):
                ad_col = f'alter table {tb_info["tb_name"]} add ' \
                    f'{tb_info["columns"][i][0]} ' \
                    f'{tb_info["columns"][i][1]} ' \
                    f'{self.abs_null(tb_info, i)}'
                try:
                    self.__cursor.execute(ad_col)
                except Exception as e:
                    print(f'error:{e}')
                    self.__conn.rollback()
        print(f'table {tb_info["tb_name"]} in database {db_info["db_name"]} has been created successfully')
    
    def create_description(self,db_name):
        self.__cursor.execute(f'use {db_name};')
        self.__cursor.execute('SHOW tables;')
        table_list_info = self.__cursor.fetchall()
        table_list_info = [i[0] for i in table_list_info]
        tb_name = f'{db_name}_description'
        if tb_name not in str(table_list_info):
            my_table = f'CREATE TABLE {tb_name}(' \
                f'symbol varchar(255) NOT NULL,' \
                f'order_book_id varchar(255) NOT NULL,' \
                f'listed_date date NOT NULL,' \
                f'maturity_date date NOT NULL,' \
                f'download varchar(255),' \
                f'update_time date,' \
                f'PRIMARY KEY (order_book_id)' \
                f')CHARSET="utf8mb4"'
            try:
                self.__cursor.execute(my_table)
            except Exception as e:
                print(f'error:{e}')
                self.__conn.rollback()
        else:
            print(f"'{tb_name}' is already created")


    def close_mysql(self):
        self.__conn.commit()
        self.__cursor.close()
        self.__conn.close()


#-------------ricequant-----------
class rq_download():
    def __init__(self):
        rq.init()
    
    def get_instrument(self,_type):
        return all_instruments(type=_type, market='cn')


    def download_tick(self,symbol,start,end):
        df_raw = get_price(symbol, start_date=start, end_date=end, frequency='tick', fields=None, adjust_type='none', market='cn', expect_df=False)
        if df_raw is None:
            return None
        else:
            df_raw = df_raw.astype(object).where(pd.notnull(df_raw), None)
            df_raw['symbol'] = symbol
            df_raw['datetime'] = df_raw.index
            df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])
            #df_raw['timestamp'] =  df_raw['datetime'].dt.time.apply(lambda x: x.strftime('%H:%M:%S.%f'))
            df_raw['timestamp'] = df_raw['datetime'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S.%f'))
            df_raw['timestamp'] = df_raw['timestamp'].str[:-3]
            df_raw['date'] = df_raw['datetime'].apply(lambda x:dt_to_d(x))
            del df_raw['trading_date']
            del df_raw['datetime']
            cols = ['date','timestamp','symbol','last','prev_settlement','prev_close','volume','open_interest','limit_up','limit_down','a1','a1_v','b1','b1_v']
            df_raw = df_raw.loc[:,cols]
            return df_raw
    
    def get_future_dominant(self,symbol,date):
        return futures.get_dominant(symbol, date)
           
###############################################################################################################################    
 #补全不规范的时间       
def fix_date(original_date):
    if re.match(r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]',original_date):
        return original_date[0:10],(original_date[11:21])
    elif re.match(r'[0-9][0-9][0-9][0-9]-[0-9][0-9]',original_date):
        return (original_date[0:7]+'-01'),(original_date[8:15]+'-01')
    elif re.match(r'[0-9][0-9][0-9][0-9]',original_date):
        return (original_date[0:4]+'-01-01'),(original_date[5:9]+'-01-01')

#transform str to datetime.date  
def to_datetime_date(date_str):
    return (datetime.date(*map(int, date_str.split('-'))))

#-----rewrite update time in database----------
def rewt_update_time(symbols):
    now = datetime.datetime.today().strftime('%Y-%m-%d')
    for symbol in symbols:
        sql_update = """
        UPDATE data_description
        SET update_time = '%s' WHERE symbol = '%s'
        """%(now,symbol)
        conn2 = pymysql.connect(host="10.3.135.12", port=3306, user="chenchen", passwd="chenchen", db="hedge_htf")
        cursor2 = conn2.cursor()
        cursor2.execute(sql_update)
        conn2.commit()
        cursor2.close()
        conn2.close()

#-------------transfer wind data to date symbol price----------
def from_edb_to_dataframe(edb):
    tp = pd.DataFrame(index=['value'], columns=edb.Times,data=edb.Data).T
    tp['symbol'] = edb.Codes[0]
    cols = list(tp)
    cols.insert(0,cols.pop(cols.index('symbol')))
    tp = tp.loc[:,cols]
    return tp

def dt_to_d(dt):
    s = dt.strftime('%Y-%m-%d')
    return datetime.datetime.strptime(s, '%Y-%m-%d').date()

def minday(d1,d2):
    delta = (d1-d2).days
    if delta>0:
        return d2
    else:
        return d1

def maxday(d1,d2):
    delta = (d1-d2).days
    if delta>0:
        return d1
    else:
        return d2