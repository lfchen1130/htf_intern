import pandas as pd

import sys
sys.path.append('D:\Program Files\Tinysoft\Analyse.NET')
import TSLPy3 as ts

from sqlalchemy import *
from tslFunctions import tslFunctions
import datetime
import dateutil as dt


class Get_cangdan_mysql(object):
    def __init__(self,start,end):
        self.__start = start
        self.__end = end


    def update_cangdan(self):
        tsl = tslFunctions()
        tsl.tsl_login()
        
        conn_Local=create_engine(str(r"mysql+pymysql://root:xxxx@localhost:3306/test"))
    
        self.__start=(dt.parser.parse(str(pd.read_sql("select max(date) from register_values",con=conn_Local\
                                                     ).values[0][0]))+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(self.__start)


        tradedaysList=tsl.getTradeDays(self.__start,self.__end)
       

    
        if len(tradedaysList)==0:
            print("已经更新至最新日期%s"%self.__end)
            return
    
      
        update_cangdan_raw=pd.DataFrame(tsl.tsbytestostr(ts.RemoteExecute(
        f'''X:=getbk('期货品种代码');
        r:=array();
        for i:=0 to length(X)-1 do
        begin
            SetSysParam(PN_Stock(),x[i]);
            Ret:=GetFuturesDailyWarehouse(inttodate({self.__start.replace('-','')}),inttodate({self.__end.replace('-','')}),t);
            r&=t;
        end;
        return r;''',{})[1]))
        
        if len(update_cangdan_raw)==0:
         	print("No Datas")
        else:
            print('1')
            can_l=update_cangdan_raw.截止日.unique()#记录下返回表的所有日期
            update_cangdan_raw['utime']=datetime.datetime.now()
            update_cangdan_raw.to_sql('register_raw',conn_Local,if_exists='append',index=False,chunksize=5000)
            
        update_cangdan=update_cangdan_raw.loc[~(update_cangdan_raw.是否小计=='小计')]
        #print(len(update_cangdan))
        update_cangdan.fillna(value=0,inplace=True)
        update_cangdan['value']=update_cangdan['已制成仓单的货物数量']+\
        update_cangdan['今日注册仓单量']+update_cangdan['仓单数量']
        update_cangdan['change']=update_cangdan['当日增减']+update_cangdan['仓单变动量']
        rlt=update_cangdan.groupby(['代码','截止日'],as_index=False)['value','change'].sum()
        rlt.columns=['code','date','value','change']
        rlt['code']=rlt['code'].apply(lambda  x: x.upper())
        rlt.to_sql('register_values',conn_Local,if_exists='append',index=False,chunksize=5000)
        ts.Disconnect()

if __name__ == "__main__":
    getdata=Get_cangdan_mysql('2019-08-01','2020-08-21')
    getdata.update_cangdan()
