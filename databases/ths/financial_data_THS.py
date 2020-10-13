from iFinDPy import * 
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np 
import datetime
import download as dl
import json 
import calendar
import time
#通过调用表最后日期和今日来确定时间段，再从时间段上挑选出所有月底的交易日
def get_last_trading_date(start,end):
    
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
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
#通过调用表最后日期和今日来确定时间段，再从时间段上挑选出所有月底，最后下载各月底的财务数据
def get_time_range_list(startdate, enddate):
    date_range_list = []
    startdate = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    enddate = datetime.datetime.strptime(enddate, '%Y-%m-%d')
    while 1:
        next_month = startdate + datetime.timedelta(days=calendar.monthrange(startdate.year, startdate.month)[1])
        month_end = next_month - datetime.timedelta(days=1)
        if month_end < enddate:
            date_range_list.append(month_end.strftime('%Y-%m-%d'))
            startdate = next_month
        else:
            return date_range_list 

def dl_income(table_name):
    #'htqh1015' '990252'
    thsLogin = THS_iFinDLogin('htqh1015','990252')#调用登录函数，账号密码登录）
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','root','ths',3306)
#    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
    
    index_name = '000300.SH'
#    db = dl.Db('192.168.1.3','root','x','ashare_new1',3306)
    symbols = db.get_symbol('index_components_ths',index_name)
    # symbols = [("000001.SZ",)]
    if not (thsLogin == 0 or thsLogin == -201):
        print("登录失败")
    else:
       
    
        for symbol in symbols[1:]:
         
            now = datetime.datetime.now()
            start_tp = db.check_financial_date(table_name,symbol[0])[0][0]
            if start_tp is not None :
                start_date = start_tp + datetime.timedelta(days=1)
                start_date = start_date.strftime('%Y-%m-%d')
            else :
                start_date = '2000-01-01'
#            start_date='2018-01-01'
#            end_date = datetime.date.today().strftime('%Y-%m-%d')
            end_date='2020-10-01'
            
            if table_name == "stock_sheet_income_ths":
                if end_date>start_date:
                    print(symbol)
                    datelist = get_time_range_list(start_date,end_date)
                    
                    for t_date in datelist:
                        print(t_date)
                        raw_data = THS_DateSerial(symbol[0],'ths_np_stock;ths_regular_report_actual_dd_stock','100;','Days:Alldays,Fill:Blank,Interval:D',t_date,t_date,True)
                        temp = json.loads(raw_data.decode('utf-8'))['tables']
                        df = pd.DataFrame(temp[0]['table'])
#                        print(len(df),'--------------------------')
                        
                        
                        df.rename(columns={'ths_np_stock':'np','ths_regular_report_actual_dd_stock':'date'},inplace=True)
                        df['report_date'] = temp[0]['time']
                        df['symbol'] = temp[0]['thscode']
                        df['ctime'] =now
                        df['utime'] = now
                        if (df.np.values[0]!='')&(df.date.values[0]!='')&(df.report_date.values[0]!=''):
                            if len(df)!=1:
                                print('error')
#                        df = df[(df.np != '')&(df.report_date!='')]
#                        df = df[df.date != '']
#                        if not df.empty:
                            print(df)
                            print('-------------------------',symbol)
                            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            elif table_name == 'stock_sheet_cash_flow_ths':
                if end_date>start_date:
                    print(symbol)
                    datelist = get_time_range_list(start_date,end_date)
                    print(datelist)
                    for t_date in datelist:
                        raw_data = THS_DateSerial(symbol[0],'ths_regular_report_actual_dd_stock;ths_ncf_from_oa_stock',';100','Days:Alldays,Fill:Blank,Interval:D',t_date,t_date,True)
                        temp = json.loads(raw_data.decode('utf-8'))['tables']
                        df = pd.DataFrame(temp[0]['table'])
                        df.rename(columns={'ths_ncf_from_oa_stock':'ncf_from_oa','ths_regular_report_actual_dd_stock':'date'},inplace=True)
                        df['report_date'] = temp[0]['time']
                        df['symbol'] = temp[0]['thscode']
                        df['ctime'] =now
                        df['utime'] = now
                        if (df.ncf_from_oa.values[0]!='')&(df.report_date.values[0]!='')&(df.date.values[0]!=''):
                            if len(df)!=1:
                                print('error')
#                        df = df[(df.ncf_from_oa != '')&(df.report_date!='')]
                            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            elif table_name == 'stock_sheet_balance_ths':
                if end_date>start_date:
                    print(symbol)
                    datelist = get_time_range_list(start_date,end_date)
                    for t_date in datelist:
                        raw_data = THS_DateSerial(symbol[0],'ths_regular_report_actual_dd_stock;ths_total_owner_equity_stock;ths_minority_equity_stock;ths_other_equity_instruments_stock;ths_total_noncurrent_liab_stock;ths_bond_payable_stock;ths_lt_staff_salary_payable_stock;ths_dt_liab_stock;ths_other_liab_stock;ths_noncurrent_liab_diff_sri_stock;ths_noncurrent_liab_diff_sbi_stock;ths_total_liab_stock;ths_total_assets_stock',';100;100;100;100;100;100;100;100;100;100;100;100','Days:Alldays,Fill:Blank,Interval:D',t_date,t_date,True)
                        temp = json.loads(raw_data.decode('utf-8'))['tables']
                        df = pd.DataFrame(temp[0]['table'])
                        df.rename(columns={'ths_regular_report_actual_dd_stock':'date','ths_total_owner_equity_stock':'total_owner_equity','ths_minority_equity_stock':'minority_equity','ths_other_equity_instruments_stock':'other_equity_instruments'\
                            ,'ths_total_noncurrent_liab_stock':'total_noncurrent_liab','ths_bond_payable_stock':'bond_payable','ths_lt_staff_salary_payable_stock':'lt_staff_salary_payable','ths_dt_liab_stock':'dt_liab'\
                            ,'ths_other_liab_stock':'other_liab','ths_noncurrent_liab_diff_sri_stock':'noncurrent_liab_diff_sri','ths_noncurrent_liab_diff_sbi_stock':'noncurrent_liab_diff_sbi','ths_total_liab_stock':'total_liab','ths_total_assets_stock':'total_assets'},inplace=True)
                        df['report_date'] = temp[0]['time']
                        df['symbol'] = temp[0]['thscode']
                        df['ctime'] =now
                        df['utime'] = now
                        df.dropna(subset=['report_date'],inplace=True)
                        if (df.date.values[0]!='')&(df.report_date.values[0]!=''):
                            if len(df)!=1:
                                print('error')
                            df = df.applymap(lambda x: np.nan if x == '' else x)
                            df = df.where(pd.notnull(df), None)
                            df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)   
            elif table_name == 'stock_dividend_indicator_ths':
                if end_date>start_date:
                    print(symbol)
                    datelist = get_time_range_list(start_date,end_date)
                    for t_date in datelist:
#                        print(t_date)
                        raw_data = THS_DateSerial(symbol[0],'ths_dividend_ps_before_tax_stock;ths_ex_dividend_date_stock;ths_dividend_sign_stock',';;','Days:Tradedays,Fill:Blank,Interval:D',t_date,t_date,True)
                        temp = json.loads(raw_data.decode('GB2312'))['tables']
                        #两类巨坑 so no 1----------------------------------------------------------------------------
                        if temp!=[]:
                        
                            df = pd.DataFrame(temp[0]['table'])
            
                            df.rename(columns={'ths_dividend_ps_before_tax_stock':'dividend_ps_before_tax','ths_ex_dividend_date_stock':'date','ths_dividend_sign_stock':'dividend_sign'},inplace=True)
                            
                            df['report_date'] = temp[0]['time']
                            df['symbol'] = temp[0]['thscode']
                            df['ctime'] =now
                            df['utime'] =now
                            #两类巨坑 so no 2----------------------------------------------------------------------------
                            if (df.dividend_sign.values[0] == '是')&(df.dividend_ps_before_tax.values[0]!='')&(df.report_date.values[0]!='')&(df.date.values[0]!=''):
                                if len(df)!=1:
                                    print('error')
    #                        df = df[(df.dividend_sign == '是')]
    #                        df = df[(df.dividend_ps_before_tax != '')&(df.report_date!='')]
                                df['dividend_sign'] = df['dividend_sign'].replace('是',1)
                                df.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
            
def get_forecast_data(table_name,forecast_year):
    #    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@192.168.1.3:3306/ashare_new1"))
    conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:x@localhost:3306/ths"))
    db = dl.Db('10.3.135.14','root','x','ths',3306)
    index_name = '000300.SH'
    
#    db = dl.Db('192.168.1.3','root','x','ashare_new1',3306)
    symbols = db.get_symbol('index_components_ths',index_name)
    count=0
    
    while count<=len(symbols):
        try:
            thsLogin = THS_iFinDLogin('htqh1015','x')#调用登录函数，账号密码登录）

        
            if not (thsLogin == 0 or thsLogin == -201):
                print("登录失败")
            else:
                now = datetime.datetime.now()
                
                for symbol in symbols[count:]:
                    
                    dflist=[]
                    count+=1
                    print(symbol[0],str(round(count/len(symbols)*100,2))+'%',count)
                    
                    start_tp = db.check_forecast_date(table_name,symbol[0],forecast_year)[0][0]
                    if start_tp is not None :
                        start_date = start_tp + datetime.timedelta(days=1)
                        start_date = start_date.strftime('%Y-%m-%d')
                    else :
                        start_date = '2005-01-01'
#                    start_date='2005-01-01'
        #            end_date = datetime.date.today().strftime('%Y-%m-%d')
                    end_date='2020-10-01'
        #            t1=time.time()
                    if end_date>start_date:
        #                    print(symbol[0])
                            datelist = get_time_range_list(start_date,end_date)
                            for t_date in datelist:
        #                        print(time.time()-t1,'---',1)
                                for i in range(3):
                                    forecast_year= datetime.datetime.strptime(t_date,'%Y-%m-%d').timetuple()[0]+i
                                    raw_data = THS_DateSerial(symbol[0],'ths_eps_fore_org_num_consensus_stock;ths_fore_np_median_consensus_stock',f'{forecast_year};{forecast_year}','Days:Tradedays,Fill:Previous,Interval:D',t_date,t_date,True)
                                    temp = json.loads(raw_data.decode('utf-8'))['tables']
                                    if temp!=[]:
        #                                print(temp[0]['table'])
                                        df = pd.DataFrame(temp[0]['table'])
                                        df.rename(columns={'ths_eps_fore_org_num_consensus_stock':'eps_fore_org_num','ths_fore_np_median_consensus_stock':'fore_np_median'},inplace=True)
                                        df['forecast_year'] = forecast_year
                                        df['date'] = temp[0]['time']
                                        df['symbol'] = temp[0]['thscode']
                                        df['ctime'] =now
                                        df['utime'] =now
                                        dflist.append(df)
        #            print(time.time()-t1,'---',2)
                    dfr=df
                    for dfs in dflist[:-1]:
                        dfr=pd.concat([dfr,dfs])
        #            print(time.time()-t1,'---',3)
                    dfr.to_sql(table_name,if_exists='append',index=False,con=conn_LOCAL_mysql)
                    
        #            print(time.time()-t1,'---',4)
            time.sleep(2)
        except Exception as e:
            print(e)
            count-=1
                        
                    
#                           
        




if __name__ == "__main__":
#    table_names = ['stock_sheet_income_ths']
    table_names = ['stock_sheet_cash_flow_ths']
#    table_names = ['stock_sheet_balance_ths']
#    table_names = ['stock_profit_forecast_ths']
    # table_names = ['stock_sheet_cash_flow_ths','stock_sheet_balance_ths','stock_sheet_income_ths']
#    table_names = ['stock_dividend_indicator_ths']
    forecast_year = 2018
    for table_name in table_names:
        dl_income(table_name)
#        get_forecast_data(table_name,forecast_year)#2020 2021 2022
