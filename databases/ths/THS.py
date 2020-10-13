from iFinDPy import * 
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json 


def write_log(symbol,e):
    with open('error.txt', "a") as f:
        f.write(symbol+'|'+e+'\n')

def dl_THS_Daily(start,end,table_name,index_symbol):
    thsLogin = THS_iFinDLogin('htqh1015','990252')#调用登录函数，账号密码登录）
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://zcb:Quant2020!@192.168.1.4:8848/ashare_new"))
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:root@localhost:3306/ths"))
    symbols = pd.read_sql(sql=f'select distinct symbol from index_components_ths where date BETWEEN "{start}" AND "{end}" and index_symbol="{index_symbol}" ',con=conn_LOCAL_mysql).values

    # symbols = [("001872.SZ",)]
#    db = dl.Db('192.168.1.4','zcb','Quant2020!','ashare_new',8848)
    db = dl.Db('10.3.135.14','root','root','ths',3306)
    if not (thsLogin == 0 or thsLogin == -201):
    # if False:
        print("登录失败")
    else:
        count=0
        
        for symbol in symbols:
            count+=1
            print(symbol,str(round(count/len(symbols)*100,2))+'%')
            try:
            # if True:
                now = datetime.datetime.now()
                start_tp = db.check_date(table_name,symbol[0])[0][0]
                if start_tp is not None :
                    start_date = start_tp + datetime.timedelta(days=1)
                    start_date = start_date.strftime('%Y-%m-%d')
                else :
                    start_date = '2000-01-01'
                end_date = datetime.date.today().strftime('%Y-%m-%d')
               
#                end_date = '2020-09-01'
                if end_date >start_date:
                    print(start_date,end_date)
                    
                
                    
#                     trading_status = THS_DateSerial(symbol[0],'ths_trading_status_stock','','Days:Tradedays,Fill:Blank,Interval:D',start_date,end_date,True)
#                     temp2 = json.loads(trading_status.decode('GB2312'))['tables']
#                     tp = pd.DataFrame(data = {'date':temp2[0]['time'],'status':temp2[0]['table']['ths_trading_status_stock']})
#        
#                     df_status = tp[tp.status != '终止上市']
#                     if not df_status.empty:
#                         df_status['status'] = df_status['status'].apply(lambda x:0 if x == '交易'else 1)
#                         status = [0 if i =='交易'else 1 for i in temp2[0]['table']['ths_trading_status_stock']]
#                         start_ = df_status['date'].min()
#                         end_ = df_status['date'].max()
                         
                         
                    if table_name == 'stock_history_day_nfq':
                        #  不复权
                        raw_data=THS_HistoryQuotes(symbol[0],'preClose;open;high;low;close;avgPrice;change;changeRatio;volume;amount;turnoverRatio;transactionAmount;totalShares;totalCapital;floatSharesOfAShares;floatSharesOfBShares;floatCapitalOfAShares;floatCapitalOfBShares;pe_ttm;pe;pb;ps;pcf','Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',start_date,end_date,True)
                    elif table_name == 'stock_history_day':
                        # 后复权（分红在投）
                        raw_data=THS_HistoryQuotes(symbol[0],'preClose;open;high;low;close;avgPrice;change;changeRatio;volume;amount;turnoverRatio;transactionAmount;totalShares;totalCapital;floatSharesOfAShares;floatSharesOfBShares;floatCapitalOfAShares;floatCapitalOfBShares;pe_ttm;pe;pb;ps;pcf','Interval:D,CPS:3,baseDate:1900-01-01,Currency:YSHB,fill:Previous',start_date,end_date,True)
                    temp = json.loads(raw_data.decode('utf-8'))['tables']
                        
                    df = pd.DataFrame(temp[0]['table'])
                    # df['date'] = df_status['date']
                    # df['status'] = df_status['status']
                    df['date'] = temp[0]['time']
                    df['symbol'] = temp[0]['thscode']
                    df['ctime'] =now
                    df['utime'] = now 
                    # df['status'] = status
                    df.rename(columns={'preClose':'pre_close','change':'change_','changeRatio':'change_ratio','turnoverRatio':'turnover_ratio','avgPrice':'avg_price','transactionAmount':'transaction_amount','totalShares':'total_shares','totalCapital':'total_capital','floatSharesOfAShares':'float_shares_of_ashares','floatSharesOfBShares':'float_shares_of_bshares'\
                    ,'floatCapitalOfAShares':'float_capital_of_ashares','floatCapitalOfBShares':'float_capital_of_bshares'},inplace=True)
                    df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            except Exception as e:
                print(symbol[0])
                print(e)
                write_log(symbol[0],str(e))
#        db.update_status(table_name)

#if __name__ == '__main__':
#    table_name = ['stock_history_day','stock_history_day_nfq']
#    for table_ in table_name:
#        print(table_)
#        dl_THS_Daily(table_)