from iFinDPy import * 
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json

def adjust(symbol):
    if symbol == 'shibor.1m':
        return 'M002816451'
    elif symbol == 'shibor.1w':
        return 'M002816449'
    elif symbol == 'shibor.1y':
        return 'M002816455'
    elif symbol == 'shibor.2w':
        return 'M002816450'
    elif symbol == 'shibor.3m':
        return 'M002816452'
    elif symbol == 'shibor.6m':
        return 'M002816453'
    elif symbol == 'shibor.9m':
        return 'M002816454'
    elif symbol == 'shibor.on':
        return 'M002816448'

    elif symbol == 'meur':
        return 'M002842092'
    elif symbol == 'mgbp':
        return 'M002842093'
    elif symbol == 'musd':
        return 'M002842089'
    elif symbol == 'mjpy':
        return 'M002842091'
    elif symbol == 'usd':
        return 'G002600864'
    elif symbol == 'gbp':
        return 'G003146350'
    elif symbol == 'eur':
        return 'G003146301'
    
    elif symbol == 'cpi':
        return 'M002826730'
    
def get_shibor(table_name):
    thsLogin = THS_iFinDLogin('htqh1015','990252')
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:Qx@192.168.1.3:3306/ashare_new1"))
#    db = dl.Db('192.168.1.3','root','Qux!','ashare_new1',3306)
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:rx@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','x','ths',3306)

    symbols = pd.read_sql(sql=f'select distinct symbol from eco_shibor_history',con=conn_LOCAL_mysql).values
    
    for symbol in symbols:
        now = datetime.datetime.now()
        start_tp = db.check_date(table_name,symbol[0])[0][0]
        if start_tp is not None :
            start_date = start_tp + datetime.timedelta(days=1)
            start_date = start_date.strftime('%Y-%m-%d')
        else :
            start_date = '2000-01-01'
        end_date = datetime.date.today().strftime('%Y-%m-%d')

        if end_date>start_date:
            print(symbol)
            symbol_ths = adjust(symbol)
            # raw_data = THS_EDBQuery(symbol_ths,start_date,end_date,True)
            raw_data = THS_EDBQuery(symbol_ths,start_date,end_date,True)
            temp = json.loads(raw_data.decode('utf-8'))['tables']
            df = pd.DataFrame(temp[0]['value'],columns=['price'])
            df['symbol'] = symbol[0]
            df['date'] = temp[0]['time']
            df['ctime'] =now
            df['utime'] = now
            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            # df.rename(columns={'ths_np_stock':'np','ths_regular_report_actual_dd_stock':'date'},inplace=True)

def get_exchange_rate(table_name):
    thsLogin = THS_iFinDLogin('htqh1015','990252')
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
#    db = dl.Db('192.168.1.3','root','x','ashare_new1',3306)
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','x','ths',3306)

    # symbols = pd.read_sql(sql=f'select distinct currency from eco_rmb_rate',con=conn_LOCAL_mysql).values
    # symbols = [("mgbp",),("meur",),("musd",),("mjpy",)]
    symbols =[("gbp",),("eur",),("usd",)]
    for symbol in symbols:
        now = datetime.datetime.now()
        start_tp = db.check_rate_date(table_name,symbol[0])[0][0]
        if start_tp is not None :
            start_date = start_tp + datetime.timedelta(days=1)
            start_date = start_date.strftime('%Y-%m-%d')
        else :
            start_date = '2000-01-01'
        end_date = datetime.date.today().strftime('%Y-%m-%d')

        if end_date>start_date:
            print(symbol)
            symbol_ths = adjust(symbol[0])
            raw_data = THS_EDBQuery(symbol_ths,start_date,end_date,True)
            temp = json.loads(raw_data.decode('utf-8'))['tables']
            df = pd.DataFrame(temp[0]['value'],columns=['rate'])
            df['currency'] = symbol[0]
            df['date'] = temp[0]['time']
            df['ctime'] =now
            df['utime'] = now
            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            
            
            
def get_cpi(table_name):
    thsLogin = THS_iFinDLogin('htqh1015','990252')
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
#    db = dl.Db('192.168.1.3','root','x','ashare_new1',3306)
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','x','ths',3306)

    # symbols = pd.read_sql(sql=f'select distinct currency from eco_rmb_rate',con=conn_LOCAL_mysql).values
    # symbols = [("mgbp",),("meur",),("musd",),("mjpy",)]
    symbols =[("cpi",)]
    for symbol in symbols:
        now = datetime.datetime.now()
        start_tp = db.check_rate_date(table_name,symbol[0])[0][0]
        if start_tp is not None :
            start_date = start_tp + datetime.timedelta(days=1)
            start_date = start_date.strftime('%Y-%m-%d')
        else :
            start_date = '2000-01-01'
        end_date = datetime.date.today().strftime('%Y-%m-%d')

        if end_date>start_date:
            print(symbol)
            symbol_ths = adjust(symbol[0])
            raw_data = THS_EDBQuery(symbol_ths,start_date,end_date,True)
            temp = json.loads(raw_data.decode('utf-8'))['tables']
            df = pd.DataFrame(temp[0]['value'],columns=['rate'])
            df['currency'] = symbol[0]
            df['date'] = temp[0]['time']
            df['ctime'] =now
            df['utime'] = now
            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)

if __name__ =='__main__':
     table_name = 'eco_shibor_history'
     get_shibor(table_name)
#    table_name = 'eco_rmb_rate'
#    get_exchange_rate(table_name)
    
    
#    table_name = 'cpi_monthly_ths'
#    get_cpi(table_name)
