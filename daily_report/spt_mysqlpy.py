import pymysql
import pandas as pd
import sqlalchemy
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pylab import mpl
import seaborn as sns
mpl.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False

now_="2020-07-23"
then_="2019-07-23"
symbols=['IF','IC','IH','TS','TF','T']
symbols1=['IF','IC','IH']
symbols2=['T','TF','TS']
index=['000300.SH','000905.SH','000016.SH']
color_1 = (219.0 / 255, 0.0 / 255, 17.0 / 255)


#流动bar图----------------------------------------------------------------------------------------------
def plots():

#con=pymysql.connect(host="localhost", port=3306, user="root", passwd="xxx", db="test")
#con=pymysql.connect(host="10.3.135.11", port=3306, user="guest", passwd="xxx", db="cash_flow")
    con = sqlalchemy.create_engine(str(r"mysql+pymysql://chenchen:xxx@10.3.135.11:3306/cash flow"))
    value_turn_over=[]
    value_open_interest=[]
    smb_tmp=pd.DataFrame({},columns=['code','value_turn_over','value_open_interest','date_range_to','date_range_oi'])
    for symbol in symbols1:
        cmd=f'SELECT * FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date DESC LIMIT 0,11'
        df=pd.read_sql(cmd,con)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[1,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[1,'position_amount'])/10**8)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[5,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[5,'position_amount'])/10**8)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[10,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[10,'position_amount'])/10**8)
    
        smb1=pd.DataFrame({'code':[f'{symbol}',f'{symbol}',f'{symbol}'],'value_turn_over':value_turn_over,'value_open_interest':\
                           value_open_interest,'date_range_to':['1日成交金额变化','5日成交金额变化','10日成交金额变化']\
                           ,'date_range_oi':['1日持仓金额变化','5日持仓金额变化','10日持仓金额变化']})
        smb1=pd.concat([smb_tmp,smb1],axis=0)
        smb_tmp=smb1
        value_turn_over=[]
        value_open_interest=[]
    
        
    smb_tmp=pd.DataFrame({},columns=['code','value_turn_over','value_open_interest','date_range_to','date_range_oi'])
    for symbol in symbols2:
        cmd=f'SELECT * FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date DESC LIMIT 0,11'
        df=pd.read_sql(cmd,con)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[1,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[1,'position_amount'])/10**8)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[5,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[5,'position_amount'])/10**8)
        value_turn_over.append((df.loc[0,'total_turnover']-df.loc[10,'total_turnover'])/10**8)
        value_open_interest.append((df.loc[0,'position_amount']-df.loc[10,'position_amount'])/10**8)
        smb2=pd.DataFrame({'code':[f'{symbol}',f'{symbol}',f'{symbol}'],'value_turn_over':value_turn_over,'value_open_interest':\
                           value_open_interest,'date_range_to':['1日成交金额变化','5日成交金额变化','10日成交金额变化']\
                           ,'date_range_oi':['1日持仓金额变化','5日持仓金额变化','10日持仓金额变化']})
        smb2=pd.concat([smb_tmp,smb2],axis=0)
        smb_tmp=smb2
        value_turn_over=[]
        value_open_interest=[]
      
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    tt1=sns.barplot(x='code', y='value_turn_over', hue='date_range_to', data=smb1[['code','value_turn_over','date_range_to']],ax=axarr,color='r')
    for ptc in axarr.patches:
        ptc.set_width(0.2)
    axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['bottom'].set_visible(False)
    axarr.spines['right'].set_visible(False)
    axarr.set_xlabel('',fontsize=4)
    axarr.set_ylabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    plt.axhline(y=0.01, color='k', linestyle='-')
    plt.savefig('spt1', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    
    
    
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    pa1 = sns.barplot(x='code', y='value_open_interest', hue='date_range_oi', data=smb1[['code','value_open_interest','date_range_oi']],ax=axarr,color='r')
    for ptc in axarr.patches:
        ptc.set_width(0.2)
    axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['bottom'].set_visible(False)
    axarr.spines['right'].set_visible(False)
    axarr.set_xlabel('',fontsize=4)
    axarr.set_ylabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    plt.axhline(y=0.01, color='k', linestyle='-')
    plt.savefig('spt2', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    tt2 = sns.barplot(x='code', y='value_turn_over', hue='date_range_to', data=smb2[['code','value_turn_over','date_range_to']],ax=axarr,color='r')
    for ptc in axarr.patches:
        ptc.set_width(0.2)
    axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['bottom'].set_visible(False)
    axarr.spines['right'].set_visible(False)
    axarr.set_xlabel('',fontsize=4)
    axarr.set_ylabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    plt.axhline(y=0.01, color='k', linestyle='-')
    plt.savefig('spt3', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    
    #
    #
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    pa2 = sns.barplot(x='code', y='value_open_interest', hue='date_range_oi', data=smb2[['code','value_open_interest','date_range_oi']],ax=axarr,color='r')
    for ptc in axarr.patches:
        ptc.set_width(0.2)
    axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['bottom'].set_visible(False)
    axarr.spines['right'].set_visible(False)
    axarr.set_xlabel('',fontsize=4)
    axarr.set_ylabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(20)
    plt.axhline(y=0.01, color='k', linestyle='-')
    plt.savefig('spt4', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    #
    #
    ###流动性line图----------------------------------------------------------------------------------------------
    ###前两幅图
    ##con=pymysql.connect(host="localhost", port=3306, user="root", passwd="xxx", db="test")
    #con = sqlalchemy.create_engine(str(r"mysql+pymysql://chenchen:xxx@10.3.135.11:3306/cash flow"))
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    i=0
    color=['r','k','gray']
    
    for symbol in symbols1:
        cmd=f'SELECT total_turnover,Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
        df[symbol+'成交金额']=df.total_turnover.apply(lambda x : x/10**8)
        df.drop(columns=['total_turnover','Date'],inplace=True)
        df.plot(kind='line',color=color[i], ax=axarr,rot=360)
        i+=1
        
        
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['right'].set_visible(False)    
    plt.savefig('spt2_1', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    #
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    i=0
    for symbol in symbols1:
        cmd=f'SELECT position_amount, Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
        df[symbol+'持仓金额']=df.position_amount.apply(lambda x : x/10**8)
        df.drop(columns=['position_amount','Date'],inplace=True)
        df.plot(kind='line',color=color[i], ax=axarr,rot=360)
        i+=1
    
    
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    axarr.spines['right'].set_visible(False)   
    plt.savefig('spt2_2', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    
    
    ##中间两幅图============================================
    ##con=pymysql.connect(host="10.3.135.14", port=3306, user="root", passwd="xxx", db="test")
    #con = sqlalchemy.create_engine(str(r"mysql+pymysql://chenchen:xxx@10.3.135.11:3306/cash flow"))
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    ax2=axarr.twinx()
    i=0
    color=['gray','k','r']
    
    for symbol in symbols2:
        cmd=f'SELECT total_turnover,Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
    #    df[symbol+'成交金额']=df.total_turnover.apply(lambda x : x/10**8)
    #    df.drop(columns=['total_turnover','Date'],inplace=True)
        if symbol=='TS':
            df[symbol+'成交金额（右轴）']=df.total_turnover.apply(lambda x : x/10**8)
            df.drop(columns=['total_turnover','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=ax2)
        else:
            df[symbol+'成交金额']=df.total_turnover.apply(lambda x : x/10**8)
            df.drop(columns=['total_turnover','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=axarr)
        i+=1
    
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.3, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    
    
    
    for tick in ax2.yaxis.get_major_ticks():
        tick.label2.set_fontsize(15)
    ax2.legend(bbox_to_anchor=(0.85, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    ax2.spines['top'].set_visible(False)
    plt.savefig('spt2_3', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti']   
      
    
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    ax2=axarr.twinx()
    i=0
    
    for symbol in symbols2:
        cmd=f'SELECT position_amount, Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
    #    df[symbol+'持仓金额']=df.position_amount.apply(lambda x : x/10**8)
    #    df.drop(columns=['position_amount','Date'],inplace=True)
        if symbol=='TS':
            df[symbol+'持仓金额（右轴）']=df.position_amount.apply(lambda x : x/10**8)
            df.drop(columns=['position_amount','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=ax2)
        else:
            df[symbol+'持仓金额']=df.position_amount.apply(lambda x : x/10**8)
            df.drop(columns=['position_amount','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=axarr)
        i+=1
        
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.3, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    
    for tick in ax2.yaxis.get_major_ticks():
        tick.label2.set_fontsize(15)
    ax2.legend(bbox_to_anchor=(0.85, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    ax2.spines['top'].set_visible(False)
    plt.savefig('spt2_4', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti']   
    
    ###后两幅图==============================================
    ##con=pymysql.connect(host="localhost", port=3306, user="root", passwd="xxx", db="test")
    #con = sqlalchemy.create_engine(str(r"mysql+pymysql://chenchen:xxx@10.3.135.11:3306/cash flow"))
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    i=0
    color=['r','k','gray']
    
    for symbol in symbols1:
        cmd=f'SELECT total_turnover,position_amount,Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
        df[symbol+'成交持仓比']=df.total_turnover/df.position_amount
        df.drop(columns=['total_turnover','position_amount','Date'],inplace=True)
        df.plot(kind='line',color=color[i],rot=360, ax=axarr)
        i+=1
     
        
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)   
    axarr.spines['right'].set_visible(False)   
    plt.savefig('spt2_5', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti']     
    
    
    
    i=0
    color=['gray','k','r']
    fig, axarr = plt.subplots(1, 1, figsize=(8,6))
    ax2=axarr.twinx()
    
    for symbol in symbols2:
        cmd=f'SELECT total_turnover,position_amount, Date FROM futures_vol_oi_daily WHERE code = "{symbol}" AND date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df=pd.read_sql(cmd,con)
        df.index=df['Date']
    #    df[symbol+'成交持仓比']=df.total_turnover/df.position_amount
    #    df.drop(columns=['total_turnover','position_amount','Date'],inplace=True)
        if symbol=='TS':
            df[symbol+'成交持仓比（右轴）']=df.total_turnover/df.position_amount
            df.drop(columns=['total_turnover','position_amount','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=ax2)
    #        continue
        else:
            df[symbol+'成交持仓比']=df.total_turnover/df.position_amount
            df.drop(columns=['total_turnover','position_amount','Date'],inplace=True)
            df.plot(kind='line',color=color[i],rot=360, ax=axarr)
        i+=1
        
    axarr.set_xlabel('',fontsize=4)
    for tick in axarr.xaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    for tick in axarr.yaxis.get_major_ticks():
        tick.label.set_fontsize(15)
    axarr.legend(bbox_to_anchor=(0.3, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    axarr.spines['top'].set_visible(False)
    
    for tick in ax2.yaxis.get_major_ticks():
        tick.label2.set_fontsize(15)
    ax2.legend(bbox_to_anchor=(0.9, 1.2), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
    ax2.spines['top'].set_visible(False)
    plt.savefig('spt2_6', bbox_inches='tight', dpi=300)
    plt.rcParams['font.sans-serif'] = ['kaiti'] 
    #
    ##
    ####排名----------------------------------------------------------------------------------------------
    ##
    def bar(df_long,df_short,symbol):
        df0=df_long.copy()
        df1=df_short.copy()
        df0.index=np.asarray(df0['name'])
        df0.drop(columns='name',inplace=True)
        df0.columns=[f'{symbol}多单排行']
        df1.index=np.asarray(df1['name'])
        df1.drop(columns='name',inplace=True)
        df1.columns=[f'{symbol}空单排行']
        
      
        fig, axarr = plt.subplots(1, 1, figsize=(8,6))
        g1=df0[:5].plot(kind='bar',color=color_1,rot=360, ax=axarr,width=0.3)
        axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
        axarr.spines['top'].set_visible(False)
        axarr.spines['right'].set_visible(False)
    
        for tick in axarr.xaxis.get_major_ticks():
            tick.label.set_fontsize(15)
        for tick in axarr.yaxis.get_major_ticks():
            tick.label.set_fontsize(20)
            
        plt.rcParams['font.sans-serif'] = ['kaiti'] 
        plt.savefig(symbol+'_long', bbox_inches='tight', dpi=300)
        
        
        fig, axarr = plt.subplots(1, 1, figsize=(8,6))
        g2=df1[:5].plot(kind='bar',color='forestgreen',rot=360, ax=axarr,width=0.3)
        axarr.legend(bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
        axarr.spines['top'].set_visible(False)
        axarr.spines['right'].set_visible(False)
    
        for tick in axarr.xaxis.get_major_ticks():
            tick.label.set_fontsize(15)
        for tick in axarr.yaxis.get_major_ticks():
            tick.label.set_fontsize(20)
        plt.rcParams['font.sans-serif'] = ['kaiti'] 
        plt.savefig(symbol+'_short', bbox_inches='tight', dpi=300)
       
        
        
        duo=[df0[:5].sum()[0],df0[:10].sum()[0],df0[:20].sum()[0]]
        kong=[df1[:5].sum()[0],df1[:10].sum()[0],df1[:20].sum()[0]]
    
        return pd.DataFrame({'合计':['前五名合计','前十名合计','前二十名合计'],'多单量':duo,'空单量':kong},\
                             index=[f'{symbol}',f'{symbol}',f'{symbol}'])
        
    
#    con=pymysql.connect(host="localhost", port=3306, user="root", passwd="root", db="test")
    con=pymysql.connect(host="10.3.135.14", port=3306, user="proxy", passwd="proxy", db="test")
    df_tmp=pd.DataFrame({},columns=['合计','多单量','空单量','多单占比(%)','空单占比(%)','净多单','净空单'])
    for symbol in symbols:
        cmd_long=f'SELECT  name,value FROM rank_long WHERE date = "{now_}"and code="{symbol}"'
        cmd_short=f'SELECT  name,value FROM rank_short WHERE date = "{now_}"and code="{symbol}"'
        df_long=pd.read_sql(cmd_long,con)
        df_short=pd.read_sql(cmd_short,con)
        df=bar(df_long,df_short,symbol)
        df['多单占比(%)']=(df.多单量/(df.多单量+df.空单量)*100).round(2)
        df['空单占比(%)']=(df.空单量/(df.多单量+df.空单量)*100).round(2)
        df['净多单']=(df.多单量-df.空单量).apply(lambda x:0 if x<0 else x )
        df['净空单']=(df.空单量-df.多单量).apply(lambda x:0 if x<0 else x )
        df=pd.concat([df_tmp,df],axis=0)
        df_tmp=df
       
    
    print(df)
    df.to_csv('rank.csv')
    #
    #
    #
    #
    #
    ###-----------------------------------------------------------------------------------------------------
    ##
    ##con1=pymysql.connect(host="localhost", port=3306, user="root", passwd="x", db="test")
    con1=pymysql.connect(host="10.3.135.13", port=3306, user="kerry", passwd="x", db="futures")
    con0=pymysql.connect(host="10.3.135.14", port=3306, user="proxy", passwd="px", db="ths")
#    con0=pymysql.connect(host="localhost", port=3306, user="root", passwd="rx", db="ths")
    color=['r','k','gray','peachpuff']
    
    for i  in range(3):
        cmd1=f'SELECT date,symbol,close FROM futures_daily_data WHERE code = "{symbols1[i]}" AND Date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        cmd0=f'SELECT date,index_symbol,close FROM index_history_day WHERE index_symbol = "{index[i]}" AND Date BETWEEN  "{then_}" AND  "{now_}" ORDER BY Date '
        df1=pd.read_sql(cmd1,con1)
        ind=pd.read_sql(cmd0,con0)
        ind.index=ind['date']
    #    print(ind)
    #    break
        fig, axarr = plt.subplots(1, 1, figsize=(8,6))
        for n in range(4):
            df=df1.groupby('date',as_index=False)[['close','date']].nth(n)
            df.index=df['date']
            df['basis']=df.close-ind.close
        
            df.drop(columns=['close','date'],inplace=True)
            
            df.plot(kind='line',color=color[n], ax=axarr,rot=360)
            lbs=[symbols1[i]+'当月基差',symbols1[i]+'次月基差',symbols1[i]+'当季基差',symbols1[i]+'次季基差']
            
        axarr.set_xlabel('',fontsize=4)
        for tick in axarr.xaxis.get_major_ticks():
            tick.label.set_fontsize(15)
        for tick in axarr.yaxis.get_major_ticks():
            tick.label.set_fontsize(15)
        
        axarr.legend(labels=lbs,bbox_to_anchor=(0.5, 1.2), ncol=2, frameon=False, prop={'size': 16}, loc=9, markerfirst=True)
       
        axarr.spines['top'].set_visible(False)
        axarr.spines['right'].set_visible(False) 
        axarr.spines['bottom'].set_visible(False)
        plt.axhline(y=0.01, color='k', linestyle='-')
        plt.savefig(symbols1[i]+'_basis', bbox_inches='tight', dpi=300)
        plt.rcParams['font.sans-serif'] = ['kaiti'] 

        
#
#  


#plots()


#---------------------------------------------------------------------------------------
#def query(sql):
#    con=pymysql.connect(host="10.3.135.13", port=3306, user="kerry", passwd="kx", db="futures")
#    cur=con.cursor()
#    cur.execute(sql)
#    return cur.fetchall()
#    cur.close()
#    con.close()
#
#now_="2010-05-07"
#symbol="AL"
#dom=query(f'SELECT  close FROM dominate WHERE date = "{now_}"and code="{symbol}" and dom_subdom="DOM"')
#day=query(f'SELECT  close FROM futures_daily_data WHERE date = "{now_}" and code= "{symbol}" order by symbol')
#
#print(dom[0])
#print(day[0])


