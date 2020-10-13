import pandas as pd

import sys
sys.path.append('D:\Program Files\Tinysoft\Analyse.NET')
import TSLPy3 as ts

from sqlalchemy import *
from tslFunctions import tslFunctions
import datetime
import dateutil as dt
import re

num = pd.read_csv('num.csv')
def name(x):
    df=num.loc[num.inst==x]
    if len(df)>0:
        return num.loc[num.inst==x].cod.values[0]
    else:
        return 0  

def rankk(df0):
    df=df0.copy()
    df['rankk']=0
    coden=df.code.unique()
    daten=df.截止日.unique()
    for cd in coden:
        for dtn in daten:
            ind=df.loc[(df.code==cd)&(df.截止日==dtn)].sort_values('数量',ascending=False).index
            df.loc[ind,'rankk']=range(1,len(ind)+1)
    return df

class Get_rank_mysql(object):
    def __init__(self,start,end):
        self.__start = start
        self.__end = end
        



    def update_rank(self):
      
        tsl = tslFunctions()
        tsl.tsl_login()
        
#        conn_Local=create_engine(str(r"mysql+pymysql://root:root@localhost:3306/test"))
        conn_Local=create_engine(str(r"mysql+pymysql://root:root@10.3.135.14:3306/test"))
    
        self.__start=(dt.parser.parse(str(pd.read_sql("select max(截止日) from rank_raw",con=conn_Local\
                                                     ).values[0][0]))+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        


        tradedaysList=tsl.getTradeDays(self.__start,self.__end)
       
        print(tradedaysList)

    
        if len(tradedaysList)==0:
            print("已经更新至最新日期%s"%self.__end)
            return
    
    
        for tday in tradedaysList:
            update_rank_raw=pd.DataFrame(tsl.tsbytestostr(ts.RemoteExecute(
            f'''endt:={tday.replace('-','')+'T'};
            X:=GetFuturesID('',endt);
            r:=array();
            for i:=0 to length(X)-1 do
            begin
            SetSysParam(PN_Stock(),x[i]);
            Ret:= GetFuturesTradeRankingByDate(endt,t);
            r&=t;
            end;
            return r;''',{})[1]))
            
            if len(update_rank_raw)==0:
             	print("No Datas")
            else:
                print(tday)
                #can_l=update_cangdan_raw.截止日.unique()#记录下返回表的所有日期
                update_rank_raw['utime']=datetime.datetime.now()
                update_rank_raw['机构简称2']=update_rank_raw['机构简称'].apply(name)
                update_rank_raw['code']=update_rank_raw.代码.apply(lambda x:re.split('\d+',x.upper())[0])
                update_rank_raw.to_sql('rank_raw',conn_Local,if_exists='append',index=False,chunksize=5000)
  
            
            #update_rank_raw.dropna(axis=1, inplace=True) 
                duo=update_rank_raw.loc[update_rank_raw.排名类型=='持买单量排名']
                kong=update_rank_raw.loc[update_rank_raw.排名类型=='持卖单量排名']
                rlt_duo=duo.groupby(['code','截止日','机构简称'],as_index=False)['数量','比上交易日增减'].sum()
                rlt_duo['rename']=rlt_duo['机构简称'].apply(name)
                rank_duo=rankk(rlt_duo)
                rank_duo.columns=['code','date','name','value','change','rename','rank']
                rank_duo.to_sql('rank_long',conn_Local,if_exists='append',index=False,chunksize=5000)
                
                rlt_kong=kong.groupby(['code','截止日','机构简称'],as_index=False)['数量','比上交易日增减'].sum()
                rlt_kong['rename']=rlt_kong['机构简称'].apply(name)
                rank_kong=rankk(rlt_kong)
                rank_kong.columns=['code','date','name','value','change','rename','rank']
                rank_kong.to_sql('rank_short',conn_Local,if_exists='append',index=False,chunksize=5000)
 
        ts.Disconnect()

if __name__ == "__main__":
    getdata=Get_rank_mysql('2020-07-17','2020-09-25')
    getdata.update_rank()