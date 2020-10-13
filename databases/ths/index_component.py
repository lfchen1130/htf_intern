from iFinDPy import * 
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json 
#root:root@localhost:3306/test"
def trading_date():
    thsLogin = THS_iFinDLogin('htqh1015','990252')
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:Quant2020!@192.168.1.3:3306/ashare_new1"))
#    db = dl.Db('192.168.1.3','root','Quant2020!','ashare_new1',3306)
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:root@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','root','ths',3306)
    if not (thsLogin == 0 or thsLogin == -201):
        print("登录失败")
    else:
        table_name = 'sys_trade_date'
        raw_data = THS_DateQuery('SZSE','dateType:0,period:D,dateFormat:0','2020-01-01','2020-12-31')
        temp = raw_data['tables']['time']
        if len(temp) != 0:
            df = pd.DataFrame(temp,columns={'trade_date'})
            df['exchange'] = 'SZSE'
            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)

def index_components(date_,symbol_):
    thsLogin = THS_iFinDLogin('htqh1015','990252')
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:Quant2020!@192.168.1.3:3306/ashare_new1"))
#    db = dl.Db('192.168.1.3','root','Quant2020!','ashare_new1',3306)
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:root@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','root','ths',3306)

    if not (thsLogin == 0 or thsLogin == -201):
    # if False:
        print("登录失败")
    else:
        now = datetime.datetime.now()
        table_name = 'index_components_ths'

        raw_data = THS_DataPool('index',f'{date_};{symbol_}','date:Y,thscode:Y',True)
        temp = json.loads(raw_data.decode('GB2312'))['tables']
        if len(temp) != 0:
            df = pd.DataFrame(temp[0]['table'])
            df.rename(columns={'DATE':'date','THSCODE':'symbol'},inplace=True)
            df['index_symbol'] = symbol_
            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
        else:
            print('no data')

def get_last_trading_date(start,end):
    
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:Quant2020!@192.168.1.3:3306/ashare_new1"))
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:root@localhost:3306/ths"))
    df = pd.read_sql(sql=f'select trade_date from sys_trade_date where exchange = "SZSE"',con=conn_LOCAL_mysql)
#    df = df[df.trade_date < datetime.date(2001,1,1)]
#    df = df[df.trade_date >= datetime.date(2000,1,1)]

    df = df[df.trade_date < datetime.date(end.timetuple()[0],end.timetuple()[1],end.timetuple()[2])]
    df = df[df.trade_date >= datetime.date(start.timetuple()[0],start.timetuple()[1],start.timetuple()[2])]

    
    
    df['year'] = df['trade_date'].apply(lambda x: x.year)
    df['month'] = df['trade_date'].apply(lambda x: x.month)
    df['day'] = df['trade_date'].apply(lambda x: x.day)
    df_end_date = df.groupby(['year','month']).max()['trade_date']
    return df_end_date
#if __name__ == '__main__':
    # '000852.SH' ZZ1000
    #  '000905.SH' #ZZ500
    # symbols = ['000852.SH','000905.SH','000300.SH']
    
    
#    symbols = ['000905.SH']
#    last_date = get_last_trading_date()
#    for symbol_ in symbols :
#        for date_ in last_date:
#            index_components(date_,symbol_)
#            break
            
    # trading_date()

        