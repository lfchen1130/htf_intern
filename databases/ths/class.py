# -*- coding: utf-8 -*-

from THS import *
from index_component import *
from index_daily import *
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json 
import time


class All():
    def __init__(self,start_date,end_date):
        self.__start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d')
        self.__end_date =  datetime.datetime.strptime(end_date,'%Y-%m-%d')
#        self.__index = index
        
        
    #   index ='000905.SH'
    def execute_components(self,index):
        symbols = [index]
        last_date=get_last_trading_date(self.__start_date,self.__end_date)
        for symbol_ in symbols :
            for date_ in last_date:
                index_components(date_,symbol_)
                
                
    # symbols = [("000852.SH",),("000905.SH",),("000300.SH",),]       
    def execute_index_daily(self,symbols):
        index_daily(symbols)
        
        
#    table_name = ['stock_history_day','stock_history_day_nfq']  
    def execute_stock_history_day(self,table_name,index_symbol):
        for table_ in table_name:
            print(table_)
            dl_THS_Daily(self.__start_date.strftime('%Y-%m-%d'),self.__end_date.strftime('%Y-%m-%d'),table_,index_symbol)
           
        
            
a=All('2020-09-01','2020-09-30')

#a.execute_components('000300.SH')
#a.execute_index_daily([("000300.SH",),])
a.execute_stock_history_day(['stock_history_day'],'000300.SH')

        

#入库sys_trade_date---------------------------------------------------

#thsLogin = THS_iFinDLogin('htqh1015','x')
##    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
##    db = dl.Db('192.168.1.3','root','x','ashare_new1',3306)
#conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
#db = dl.Db('10.3.135.14','root','x','ths',3306)
#now = datetime.datetime.now()
#if not (thsLogin == 0 or thsLogin == -201):
#    print("登录失败")
#else:
#    table_name = 'sys_trade_date'
#    raw_data = THS_DateQuery('SZSE','dateType:0,period:D,dateFormat:0','1999-01-01','2021-12-31')
##    print(raw_data)
#    temp = raw_data['tables']['time']
##    print(temp)
#    if len(temp) != 0:
#        df = pd.DataFrame(temp,columns={'trade_date'})
#        df['exchange'] = 'SZSE'
#        df['ctime'] =now
#        df['utime'] = now    
#        df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)


#测试代码------------------------------------------------------

def get_last_trading_date(start,end):
    start=datetime.datetime.strptime(start,'%Y-%m-%d')
    end=datetime.datetime.strptime(end,'%Y-%m-%d')

    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
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



print(get_last_trading_date('2020-01-01','2020-05-01'))
