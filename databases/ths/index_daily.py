from iFinDPy import * 
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json 

def index_daily(symbols):

    thsLogin = THS_iFinDLogin('htqh1015','990252')#调用登录函数，账号密码登录）
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
    # symbols = pd.read_sql(sql=f'select distinct code from index_components',con=conn_LOCAL_mysql).values
    # print(symbols)
#    db = dl.Db('192.168.1.3','root','x!','ashare_new1',3306)
    db = dl.Db('10.3.135.14','root','x','ths',3306)
    if not (thsLogin == 0 or thsLogin == -201):
    # if False:
        print("登录失败")
    else:
        table_name = 'index_history_day'
        
        # 000300.SH
        # symbols = [("000852.SH",),("000905.SH",),("000300.SH",),]
#        symbols = [("000300.SH",),]
        for symbol in symbols:
            print(symbol)
            if True:
                now = datetime.datetime.now()
                start_tp = db.check_index_date(table_name,symbol[0])[0][0]
                if start_tp is not None :
                    start_date = start_tp + datetime.timedelta(days=1)
                    start_date = start_date.strftime('%Y-%m-%d')
                else :
                    start_date = '2000-01-01'
#                end_date = datetime.date.today().strftime('%Y-%m-%d')
                end_date = datetime.date.today()- datetime.timedelta(days=1)
                end_date = end_date.strftime('%Y-%m-%d')
                if end_date >start_date:
        
                    raw_data=THS_HistoryQuotes(symbol[0],'preClose;open;high;low;close;avgPrice;change;changeRatio;volume;turnoverRatio;totalCapital;floatCapital;pe_ttm_index;pb_mrq;pe_indexPublisher','Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',start_date,end_date,True)
                    temp = json.loads(raw_data.decode('utf-8'))['tables']
                        
                    df = pd.DataFrame(temp[0]['table'])
                    # df['date'] = df_status['date']
                    # df['status'] = df_status['status']
                    df['date'] = temp[0]['time']
                    df['index_symbol'] = temp[0]['thscode']
                    df['ctime'] =now
                    df['utime'] = now              
                    # df['status'] = status
                    df.rename(columns={'preClose':'pre_close','change':'change_','changeRatio':'change_ratio','turnoverRatio':'turnover_ratio','avgPrice':'avg_price','totalCapital':'total_capital','floatCapital':'float_capital','pe_indexPublisher':'pe_index_publisher'},inplace=True)
                    df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
                else:
                    print('no data')
                
 
