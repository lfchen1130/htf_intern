# -*- coding: utf-8 -*-
import pandas as pd
import numpy
import rqdatac
from rqdatac import *
import rqdatac
import pymysql
import sqlalchemy
import datetime
from openpyxl import load_workbook
from WindPy import w
import time
#import PlotFigure as pf

# Start the rice quant
#rqdatac.init()

# connect mysql
dfs={}
symbols=['IF','IC','IH','TS','TF','T']
##conn_LOCAL_mysql = sqlalchemy.create_engine(str(r"mysql+pymysql://root:root@localhost:3306/test"))
conn_LOCAL_mysql  = sqlalchemy.create_engine(str(r"mysql+pymysql://chenchen:chenchen@10.3.135.11:3306/cash flow"))
df = pd.read_sql(sql=f'SELECT * FROM futures_vol_oi_daily WHERE code in ("IF","IC","IH","TS","TF","T") ORDER BY date DESC limit 0,12',con=conn_LOCAL_mysql)
for symbol in symbols:
    df0=df[df.code==symbol].copy()
    if symbol=='IF':
        df0['txt']='沪深300期货'
    if symbol=='IC':
        df0['txt']='中证500期货'
    if symbol=='IH':
        df0['txt']='上证50期货'
    dfs[symbol]=df0

#print(dfs['IF'].Date.values[0])


#tradedaysList = get_trading_dates(start_date='2020-01-06', end_date='2020-01-06')

#sql_str4 = "select futures_name.big_category,futures_name.category,futures_name.refer_name,futures_vol_oi_daily.* from " \
#           "futures_name INNER JOIN  " \
#           "futures_vol_oi_daily ON futures_name.underlying_symbol = futures_vol_oi_daily.underlying_symbol  " \
#           "where futures_vol_oi_daily.Date >='2014-01-01'"
#futures_vol_oi = pd.read_sql(sql_str4, con=engine)
#data_all = futures_vol_oi.loc[(futures_vol_oi["big_category"] == "商品")]

# 取品种成交额数据(亿元)
#volume = data_all[['Date', 'refer_name', 'total_turnover']]
#volume = volume.set_index(['Date', 'refer_name'])
#volume = volume.unstack()
#volume = volume['total_turnover'] / 100000000
#volume = volume.sort_index(ascending=False)
## 取品种净持仓额变化数据(万元)
#position_amount = data_all[['Date', 'refer_name', 'position_amount']]
#position_amount = position_amount.set_index(['Date', 'refer_name'])
#position_amount = position_amount.unstack()
#position_amount = position_amount['position_amount'] / 10000
#position_amount = position_amount.sort_index(ascending=False)
#position_change = position_amount.diff(-1, axis=0)  # position change
## 取品种持仓额数据(亿元)
#position_data = position_amount / 10000
## 取板块成交额数据(亿元)
#groups_volume = data_all[['Date', 'category', 'total_turnover']]
#groups_volume = groups_volume.groupby(['Date', 'category']).sum()
#groups_volume = groups_volume.unstack()
#groups_volume = groups_volume['total_turnover'] / 100000000
#groups_volume = groups_volume.sort_index(ascending=False)
## 取板块成交额月度数据(亿元)
#groups_volume_month = groups_volume.resample('M').sum()
#groups_volume_month['month'] = groups_volume_month.index.month
#groups_volume_month['year'] = groups_volume_month.index.year
#groups_volume_month = groups_volume_month.set_index(['month', 'year'])
#groups_volume_month = groups_volume_month.unstack()
## 取板块净持仓额变化数据(万元)
#groups_position = data_all[['Date', 'category', 'position_amount']]
#groups_position = groups_position.groupby(['Date', 'category']).sum()
#groups_position = groups_position.unstack()
#groups_position = groups_position.sort_index(ascending=False)
#groups_position_data = groups_position.diff(-1, axis=0)  # position change
#groups_position_data = groups_position_data / 10000
## 取板块持仓额数据(亿元)
#groups_position = groups_position / 100000000
#groups_position = groups_position['position_amount']
## 取板块wind商品指数数据
#sql_str5 = "select * from futures_wind_index where Date >='2015-01-01'"
#futures_wind_index = pd.read_sql(sql_str5, con=engine)
#wind_index = futures_wind_index[['date', 'name', 'value']]
#wind_index = wind_index.set_index(['date', 'name'])
#wind_index = wind_index.unstack()
#wind_index = wind_index.sort_index(ascending=False)
#wind_index = wind_index['value']
#
#
## 定义函数，取函数前period交易日滚动base数据、增长率
#def rank_data_volume_analysis(base, period):
#    temp = {'current': base[0:0 + period].apply(lambda x: x.sum()),
#            'before': base[1:1 + period].apply(lambda x: x.sum())}
#    df = pd.DataFrame(data=temp, columns=('current', 'before')).T
#    df.loc['RaiseRate'] = df.apply(lambda x: None if (x[1] == 0) else (x[0] - x[1]) / x[1], axis=0)
#    df = df.sort_values(by='current', axis=1, ascending=False)
#    return df
#
#
#def rank_data_position_analysis(base, period):
#    # temp = {'current': base[0:1].apply(lambda x: x.sum()),
#    #         'before': base[period:(1+period)].apply(lambda x: x.sum()}
#    # df = pd.DataFrame(data=temp, columns=('current', 'before')).T
#    # df.loc['RaiseRate'] = df.apply(lambda x: None if (x[1] == 0) else (x[0] - x[1]) / x[1], axis=0)
#    # df = df.sort_values(by='current', axis=1, ascending=False)
#    temp = {'current': base[0:1].apply(lambda x: x.sum()),
#            'before': base[period:1 + period].apply(lambda x: x.sum())}
#    df = pd.DataFrame(data=temp, columns=('current', 'before')).T
#    df.loc['current'] = df.loc['current'] - df.loc['before']
#    df.loc['RaiseRate'] = df.apply(lambda x: None if (x[1] == 0) else (x[0]) / x[1], axis=0)
#    df = df.sort_values(by='current', axis=1, ascending=False)
#    return df
#
#
## 构建第一部分图形基础数据
#new_volume_single = rank_data_volume_analysis(volume, 1)  # 图2：前一交易日成交金额前十名品种成交情况
#five_volume_single = rank_data_volume_analysis(volume, 5)  # 图6：5日滚动成交金额前十名品种成交情况
#ten_volume_single = rank_data_volume_analysis(volume, 10)  # 图10：10日滚动成交金额前十名品种成交情况
#new_volume_group = rank_data_volume_analysis(groups_volume, 1)  # 图4：前一交易日各板块成交金额情况
#five_volume_group = rank_data_volume_analysis(groups_volume, 5)  # 图8：5日滚动各板块成交金额情况
#ten_volume_group = rank_data_volume_analysis(groups_volume, 10)  # 图12：10日滚动各板块成交金额情况
#new_position_single = rank_data_position_analysis(position_data, 1)  # 图1：前一交易日增减仓前五名品种持仓金额变化情况
#five_position_single = rank_data_position_analysis(position_data, 5)  # 图5：5日滚动增减仓前五名品种持仓金额变化情况
#ten_position_single = rank_data_position_analysis(position_data, 10)  # 图9：10日滚动增减仓前五名品种持仓金额变化情况
#new_position_group = rank_data_position_analysis(groups_position, 1)  # 图3：前一交易日各板块持仓金额变化情况
#five_position_group = rank_data_position_analysis(groups_position, 5)  # 图7：5日滚动各板块持仓金额变化情况
#ten_position_group = rank_data_position_analysis(groups_position, 10)  # 图11：10日滚动各板块持仓金额变化情况
#
## 构建第二部分图形基础数据
## 构建总持仓金额、总成交金额与Wind指数数据集
#volume_total = groups_volume.apply(sum, axis=1)
#position_total = groups_position.apply(sum, axis=1)
#market = {'成交金额（亿元）': volume_total, '持仓金额（亿元）': position_total}
#market = pd.DataFrame(data=market)
#market['Wind商品指数'] = wind_index['商品指数']
#market = market.sort_index(ascending=False)
## 构建板块成交金额、持仓金额、wind指数
#dict_test = {}  # dict 板块名：DataFrame数据表
#for i in ['有色', '油脂油料', '软商品', '农副产品', '谷物', '贵金属', '能源', '煤焦钢矿',
#          '非金属建材', '化工']:
#    test = pd.DataFrame(groups_volume[i])
#    test.columns = ['成交金额（亿元）']
#    test['持仓金额（亿元）'] = groups_position[i]
#    test['Wind' + i + '指数(右轴)'] = wind_index[i]
#    dict_test[i] = test
#
## 开始做图
#pf.plot_bar_figure(new_volume_single.iloc[:, 0:10], '成交额(亿元)', '成交额变化率(右轴)', '图2：前一交易日成交金额前十名品种成交情况.png')
#pf.plot_bar_figure(five_volume_single.iloc[:, 0:10], '五日累计成交额(亿元)', '成交额变化率(右轴)', '图6：5日滚动成交金额前十名品种成交情况.png')
#pf.plot_bar_figure(ten_volume_single.iloc[:, 0:10], '十日累计成交额(亿元)', '成交额变化率(右轴)', '图10：10日滚动成交金额前十名品种成交情况.png')
#pf.plot_bar_figure(new_volume_group.iloc[:, 0:10], '成交额(亿元)', '成交额变化率(右轴)', '图4：前一交易日各板块成交金额情况况.png')
#pf.plot_bar_figure(five_volume_group.iloc[:, 0:10], '五日累计成交额(亿元)', '成交额变化率(右轴)', '图8：5日滚动各板块成交金额情况.png')
#pf.plot_bar_figure(ten_volume_group.iloc[:, 0:10], '十日累计成交额(亿元)', '成交额变化率(右轴)', '图12：10日滚动各板块成交金额情况.png')
#pf.plot_bar_figure(
#    pd.DataFrame(pd.concat([new_position_single.iloc[:, 0:5], new_position_single.iloc[:, -5:]], axis=1)),
#    '持仓金额变化(亿元)', '持仓金额变化率(右轴)', '图1：前一交易日增减仓前五名品种持仓金额变化情况.png')
#pf.plot_bar_figure(
#    pd.DataFrame(pd.concat([five_position_single.iloc[:, 0:5], five_position_single.iloc[:, -5:]], axis=1)),
#    '五日累计持仓金额变化(亿元)', '持仓金额变化率(右轴)', '图5：5日滚动增减仓前五名品种持仓金额变化情况.png')
#pf.plot_bar_figure(
#    pd.DataFrame(pd.concat([ten_position_single.iloc[:, 0:5], ten_position_single.iloc[:, -5:]], axis=1)),
#    '十日累计持仓金额变化(亿元)', '持仓金额变化率(右轴)', '图9：10日滚动增减仓前五名品种持仓金额变化情况.png')
#pf.plot_bar_figure(pd.DataFrame(pd.concat([new_position_group.iloc[:, 0:5], new_position_group.iloc[:, -5:]], axis=1)),
#                   '持仓金额(亿元)', '持仓金额变化率(右轴)', '图3：前一交易日各板块持仓金额变化情况.png')
#pf.plot_bar_figure(
#    pd.DataFrame(pd.concat([five_position_group.iloc[:, 0:5], five_position_group.iloc[:, -5:]], axis=1)),
#    '五日累计持仓金额变化(亿元)', '持仓金额变化率(右轴)', '图7：5日滚动各板块持仓金额变化情况.png')
#pf.plot_bar_figure(pd.DataFrame(pd.concat([ten_position_group.iloc[:, 0:5], ten_position_group.iloc[:, -5:]], axis=1)),
#                   '十日累计持仓金额变化(亿元)', '持仓金额变化率(右轴)', '图11：10日滚动各板块持仓金额变化情况.png')
#pf.plot_figure(groups_position.iloc[0:252, :], '图13： 各版块持仓金额(亿元)')
#pf.plot_figure(groups_volume.iloc[0:252, :], '图14： 各版块成交金额(亿元)')
#pf.fill_plot_figure(market.iloc[0:252, :], '图15：中国商品期货市场持仓金额、成交金额与Wind商品指数')
#title = ['图16：有色板块持仓金额、成交金额与Wind有色指数',
#         '图17：油脂油料板块持仓金额、成交金额与Wind油脂油料指数', '图18：软商品板块持仓金额、成交金额与Wind软商品指数',
#         '图19：农副产品板块持仓金额、成交金额与Wind农副产品指数', '图20：谷物板块持仓金额、成交金额与Wind谷物指数',
#         '图21：贵金属板块持仓金额、成交金额与Wind贵金属指数', '图22：能源板块持仓金额、成交金额与Wind能源指数',
#         '图23：煤焦钢矿持仓金额、成交金额与Wind煤焦钢矿指数', '图24：非金属建材持仓金额、成交金额与Wind非金属建材指数',
#         '图25：化工板块持仓金额、成交金额与Wind化工指数']
#group_name = ['有色', '油脂油料', '软商品', '农副产品', '谷物', '贵金属', '能源', '煤焦钢矿',
#              '非金属建材', '化工']
#for i in range(0, 10):
#    pf.fill_plot_figure(dict_test[group_name[i]].iloc[0:252, :], title[i])
#
## 板块月度成交额图片
#nam_dic = {'有色': '图26：有色板块月度成交金额(亿元)',
#           '油脂油料': '图27：油脂油料板块月度成交金额(亿元)',
#           '软商品': '图28：软商品板块月度成交金额(亿元)',
#           '农副产品': '图29：农副产品板块月度成交金额(亿元)',
#           '谷物': '图30：谷物板块月度成交金额(亿元)',
#           '贵金属': '图31：贵金属板块月度成交金额(亿元)',
#           '能源': '图32：能源板块月度成交金额(亿元)',
#           '煤焦钢矿': '图33：煤焦钢矿板块月度成交金额(亿元)',
#           '非金属建材': '图34：非金属建材板块月度成交金额(亿元)',
#           '化工': '图35：化工板块月度成交金额(亿元)',
#           }
#for i in groups_volume_month.columns.levels[0]:
#    pf.plot_month_figure(groups_volume_month[i], nam_dic[i])
#
#
## 生成报告需要的日期、文字
## 取前一交易日、前六交易日、前11交易日日期
#def get_date():
#    date = [volume.index[0].strftime("%Y-%m-%d"), volume.index[5].strftime("%Y-%m-%d"),
#            volume.index[10].strftime("%Y-%m-%d")]
#    return date
#
#
## 更新报告第一页文字内容
def rep():
    # if abs(new_position_group.iloc[0][1]) > abs(new_position_group.iloc[-1][1]):
    #     rep0 = new_position_group.columns.values[0] + '增仓首位'
    # else:
#         rep0 = new_position_group.columns.values[-1] + '减仓首位'
    rep0=[]
    mk=[]

    for smb in ['IF','IC','IH']:
        if  dfs[smb].position_amount.values[0]- dfs[smb].position_amount.values[1] < 0:
            mk.append(1)
            lst=dfs[smb].txt.values[0]
            rep0.append(f'{lst}资金流出')
        else:
            mk.append(0)
            lst=dfs[smb].txt.values[0]
            rep0.append(f'{lst}资金流入')
    if (mk[0]==1)&(mk[1]==1)&(mk[2]==1):
        rep0='股指期货资金流出'
    elif (mk[0]==0)&(mk[1]==0)&(mk[2]==0):
        rep0='股指期货资金流入'
    else:
        rep0=rep0[0]+','+rep0[1]+','+rep0[2]
   
    rep1 =str( datetime.datetime.now().timetuple()[0])+'年'+str( datetime.datetime.now().timetuple()[1])+'月'+str( datetime.datetime.now().timetuple()[2])+'日'\
            + '，' +'沪深300期货(IF)' + '成交' + str(
            abs(round(dfs['IF'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['IF'].total_turnover.values[0]- dfs['IF'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['IF'].total_turnover.values[0]- dfs['IF'].total_turnover.values[1])/ dfs['IF'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['IF'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['IF'].position_amount.values[0]- dfs['IF'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['IF'].position_amount .values[0]- dfs['IF'].position_amount .values[1])/dfs['IF'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['IF'].total_turnover.values[0]/ dfs['IF'].position_amount.values[0],2)))+'。'\
                +'中证500期货(IC)' + '成交' + str(
            abs(round(dfs['IC'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['IC'].total_turnover.values[0]- dfs['IC'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['IC'].total_turnover.values[0]- dfs['IC'].total_turnover.values[1])/ dfs['IC'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['IC'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['IC'].position_amount.values[0]- dfs['IC'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['IC'].position_amount .values[0]- dfs['IC'].position_amount .values[1])/dfs['IC'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['IC'].total_turnover.values[0]/ dfs['IC'].position_amount.values[0],2)))+'。'\
                +'上证50(IH)' + '成交' + str(
            abs(round(dfs['IH'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['IH'].total_turnover.values[0]- dfs['IH'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['IH'].total_turnover.values[0]- dfs['IH'].total_turnover.values[1])/ dfs['IH'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['IH'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['IH'].position_amount.values[0]- dfs['IH'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['IH'].position_amount .values[0]- dfs['IH'].position_amount .values[1])/dfs['IH'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['IH'].total_turnover.values[0]/ dfs['IH'].position_amount.values[0],2)))+'。'
                
    rep2 =str( datetime.datetime.now().timetuple()[0])+'年'+str( datetime.datetime.now().timetuple()[1])+'月'+str( datetime.datetime.now().timetuple()[2])+'日'\
            + '，' +'2年期债(TS)' + '成交' + str(
            abs(round(dfs['TS'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['TS'].total_turnover.values[0]- dfs['TS'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['TS'].total_turnover.values[0]- dfs['TS'].total_turnover.values[1])/ dfs['TS'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['TS'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['TS'].position_amount.values[0]- dfs['TS'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['TS'].position_amount .values[0]- dfs['TS'].position_amount .values[1])/dfs['TS'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['TS'].total_turnover.values[0]/ dfs['TS'].position_amount.values[0],2)))+'。'\
                +'5年期债(TF)' + '成交' + str(
            abs(round(dfs['TF'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['TF'].total_turnover.values[0]- dfs['TF'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['TF'].total_turnover.values[0]- dfs['TF'].total_turnover.values[1])/ dfs['TF'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['TF'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['TF'].position_amount.values[0]- dfs['TF'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['TF'].position_amount .values[0]- dfs['TF'].position_amount .values[1])/dfs['TF'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['TF'].total_turnover.values[0]/ dfs['TF'].position_amount.values[0],2)))+'。'\
                +'10年期债(T)' + '成交' + str(
            abs(round(dfs['T'].total_turnover.values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if ( dfs['T'].total_turnover.values[0]- dfs['T'].total_turnover.values[1]) < 0 else '增加')  +str(
             abs(round(100 *( dfs['T'].total_turnover.values[0]- dfs['T'].total_turnover.values[1])/ dfs['T'].total_turnover.values[1], 2)) )\
                + '%；' + '持仓金额' + str(
            abs(round( dfs['T'].position_amount .values[0]/100000000, 2))) \
               + '亿元，较上一交易日' + ('减少' if  dfs['T'].position_amount.values[0]- dfs['T'].position_amount.values[1] < 0 else '增加') + str(
             abs(round(100 *( dfs['T'].position_amount .values[0]- dfs['T'].position_amount .values[1])/dfs['T'].position_amount.values[1],2))) + '%；' +\
                '成交持仓比为'+str(abs(round(dfs['T'].total_turnover.values[0]/ dfs['T'].position_amount.values[0],2)))+'。'
    report = [ rep0, rep1, rep2]
                
    return report
rep()
#print(rep()[0],rep()[1],rep()[2])

#    rep0 = new_position_single.columns.values[0] + '增仓首位，' + new_position_single.columns.values[-1] + '减仓首位'
#    rep1 = get_date()[0] + '，' + new_position_single.columns.values[-1] + '减仓' + str(
#        abs(round(new_position_single.iloc[0][-1], 2))) \
#           + '亿元，环比' + ('减少' if new_position_single.loc['RaiseRate'][-1] < 0 else '增加') + str(
#         abs(round(100 *new_position_single.loc['RaiseRate'][-1], 2))) + '%，位于当日全品种减仓排名首位。' + \
#           new_position_single.columns.values[0] + '增仓' + str(round(new_position_single.iloc[0][0], 2)) + '亿元，环比' \
#           + ('减少' if new_position_single.loc['RaiseRate'][0] < 0 else '增加') + str(
#        abs(round(new_position_single.loc['RaiseRate'][0] * 100, 2))) \
#           + '%，位于当日全品种增仓排名首位；' + \
#           (five_position_single.columns.values[0] if five_position_single.columns.values[0] ==
#                                                      ten_position_single.columns.values[0] \
#                else (five_position_single.columns.values[0] + '、' + ten_position_single.columns.values[0])) + \
#           '5日、10日滚动增仓最多；' + (five_position_single.columns.values[-1] if five_position_single.columns.values[-1] ==
#                                                                         ten_position_single.columns.values[-1] \
#                                  else (
#            five_position_single.columns.values[-1] + '、' + ten_position_single.columns.values[-1])) + \
#           '5日、10日滚动减仓最多。成交金额上，' + new_volume_single.columns.values[0] + \
#           '、' + new_volume_single.columns.values[1] + '、' + new_volume_single.columns.values[2] + '分别成交' + str(
#        round(new_volume_single.iloc[0][0], 2)) \
#           + '亿元、' + str(round(new_volume_single.iloc[0][1], 2)) + '亿元和' + str(round(new_volume_single.iloc[0][2], 2)) + \
#           '亿元（环比：' + str(round(100 * new_volume_single.loc['RaiseRate'][0], 2)) + '%、' + str(
#        round(100 * new_volume_single.loc['RaiseRate'][1], 2)) \
#           + '%、' + str(round(100 * new_volume_single.loc['RaiseRate'][2], 2)) + '%)，位于当日成交金额排名前三名。'
#    rep2 = '本报告板块划分采用Wind大类商品划分标准，共分为有色、油脂油料、软商品、农副产品、能源、煤焦钢矿、化工、贵金属、谷物' \
#           '和非金属建材10个板块。' + get_date()[0] + '，' + new_position_group.columns.values[0] + '板块位于增仓首位；' \
#           + new_position_group.columns.values[-1] + '板块位于减仓首位。成交量上' + new_volume_group.columns.values[0] + \
#           '、' + new_volume_group.columns.values[1] + '、' + new_volume_group.columns.values[2] + '分别成交' + str(
#        round(new_volume_group.iloc[0][0], 2)) \
#           + '亿元、' + str(round(new_volume_group.iloc[0][1], 2)) + '亿元和' + str(round(new_volume_group.iloc[0][2], 2)) + \
#           '亿元，位于当日板块成交金额排名前三；' + new_volume_group.columns.values[-1] + \
#           '、' + new_volume_group.columns.values[-2] + '、' + new_volume_group.columns.values[-3] + '板块成交低迷。'
#    report = [rep0, rep1, rep2]
#    return report