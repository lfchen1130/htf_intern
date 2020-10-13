import sys
sys.path.append('D:\Program Files\Tinysoft\Analyse.NET')
import TSLPy3 as ts
import pandas as pd
import datetime
# from mongodb_functions import mongodb_client
import re


class CtaTickData(object):
    """Tick数据"""
    # ----------------------------------------------------------------------
    def __init__(self):
        EMPTY_STRING = ''
        EMPTY_FLOAT = 0.0
        EMPTY_INT = 0

        """Constructor"""
        self.vtSymbol = EMPTY_STRING  # vt系统代码 CF705 StockName
        self.symbol = EMPTY_STRING  # 合约代码   CF1705 StockID
        self.exchange = EMPTY_STRING  # 交易所代码


        # 成交数据
        self.lastPrice = EMPTY_FLOAT  # 最新成交价
        self.volume = EMPTY_INT  # 最新成交量
        self.amount = EMPTY_INT #成交金额
        self.cjbs = EMPTY_INT  # 周期内成交笔数
        self.yclose = EMPTY_FLOAT #上一收盘价
        self.preSettlement = EMPTY_FLOAT #上一日结算价

        self.preOpenInterest = EMPTY_INT  # 昨持仓量
        self.openInterest = EMPTY_INT  # 持仓量

        self.upperLimit = EMPTY_FLOAT  # 涨停价
        self.lowerLimit = EMPTY_FLOAT  # 跌停价

        # tick的时间
        self.tradingDay = EMPTY_STRING  # 交易日期
        self.date = EMPTY_STRING  # 日期
        self.time = EMPTY_STRING  # 时间
        self.datetime = None  # python的datetime时间对象

        # 五档行情
        self.bidPrice1 = EMPTY_FLOAT
        self.askPrice1 = EMPTY_FLOAT
        self.bidVolume1 = EMPTY_INT
        self.askVolume1 = EMPTY_INT

class CtaDailyData(object):
    """Tick数据"""
    # ----------------------------------------------------------------------
    def __init__(self):
        EMPTY_STRING = ''
        EMPTY_FLOAT = 0.0
        EMPTY_INT = 0

        """Constructor"""
        self.vtSymbol = EMPTY_STRING
        self.commodity = EMPTY_STRING
        self.date = EMPTY_STRING


        # 成交数据
        self.close = EMPTY_FLOAT
        self.open = EMPTY_FLOAT
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT

        self.settlement = EMPTY_FLOAT
        self.volume = EMPTY_INT
        self.openint = EMPTY_FLOAT
        self.margin = EMPTY_FLOAT





class tslFunctions():
    def __init__(self):
        self.mc = None
    #登录
    def tsl_login(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("htccqh", "Htfc2020!")  # Tuple(ErrNo,ErrMsg) 登陆用户
        if dl[0] == 0:
            print("登陆成功")
            print("服务器设置:", ts.GetService())
            ts.SetComputeBitsOption(64)  # 设置计算单位
            print("计算位数设置:", ts.GetComputeBitsOption())
        else:
            print(dl[1])

    #STR转GBK
    def tsbytestostr(self, data):
        if isinstance(data, (tuple, list)):
            lendata = len(data)
            ret = []
            for i in range(lendata):
                ret.append(self.tsbytestostr(data[i]))
        elif isinstance(data, dict):
            ret = {}
            for i in data:
                ret[self.tsbytestostr(i)] = self.tsbytestostr(data[i])
        elif isinstance(data, bytes):
            ret = data.decode('gbk')
        else:
            ret = data
        return ret

    #取交易日LIST
    def getTradeDays(self,start,end):
        daysList = self.tsbytestostr(ts.RemoteExecute(
            "begt:=strtodate('%s');endt:=strtodate('%s');return datetostr(spec(specdate(nday3(tradedays(begt,endt),sp_time()),endt),'SH000001'));" % (
                start, end), {}))
        return daysList[1]

    def getDailyFuturesCode(self,BegDate,EndDate,section='郑州商品交易所;上海期货交易所;大连商品交易所;中国金融期货交易所;上海国际能源交易中心'):
        allContractsCode =pd.DataFrame(self.tsbytestostr(ts.RemoteExecute(
            '''return select thisrow as '代码', spec(datetostr(inttodate(base(703018))),thisrow) as '最后交易日',\
            spec(datetostr(inttodate(base(703002,0))),thisrow) as '变动日'\
             from getbk('%s') end; '''%section,{}))[1])
        specDayContracts1 = allContractsCode[allContractsCode['变动日']<=EndDate]
        specDayContracts2 = specDayContracts1[specDayContracts1['最后交易日'] >= BegDate]
        return specDayContracts2['代码']

    def getDailyFuturesDetails(self,BegDate,EndDate,symbol):
        detailsDF=pd.DataFrame(self.tsbytestostr(ts.RemoteExecute(
            '''N:=TradeDays(StrToDate('%s'),StrToDate('%s'));
            return nday(N, 'DATE', datetimetostr(sp_time()),
                        'CLOSE', close(),
                        'HIGH', high(),
                        'LOW', low(),
                        'OPEN',open(),
                        'AMOUNT',amount(),
                        "SETTLEMENT",Settlement(),
                        "OPENINT",OpenInterest(),
                        "VOL",vol(),
                        'CONTRACT',base(703001),
                        'COMMODITY',base(703003),
                        'MARGIN',FuturesTradingMarginRate(sp_time(),0));'''%(BegDate,EndDate),
            {'StockID': symbol, "CurrentDate": ts.LocalCallFunc("StrToDate", [EndDate])[1]}))[1])
        try:
            detailsDF['COMMODITY'] = [x.upper() for x in detailsDF['COMMODITY']]
        except Exception as e:
            print('-------------------')
            print(e)
            print('%s合约没有数据！！！'%symbol)
            print('-------------------')
        return detailsDF

    def getTickDetails(self, BegDate, EndDate, contract_code,symbol):
        # tick_details = pd.DataFrame(self.tsbytestostr(ts.RemoteExecute( '''a:=select ["StockID"],["StockName"],["date"],["price"],["vol"],["amount"],["cjbs"],["yclose"],["syl1"],["syl2"],["buy1"],["sale1"],["bc1"],["sc1"] from tradetable datekey %s+21/24 to %s+16/24 of '%s' end;b:=update a set ['date']=datetimetostr(['date']) end;return a;''' % (
        # ts.LocalCallFunc("StrToDate", [BegDate])[1], ts.LocalCallFunc("StrToDate", [EndDate])[1], contract_code),{}))[1])
        # tick_details.to_csv('%s%s_%s.csv'%(path,symbol,EndDate),index=False)
        # return tick_details

        # tick_details = pd.DataFrame(self.tsbytestostr(ts.RemoteExecute(
        # '''a:=select ["StockID"],["StockName"],["date"],["price"],["vol"],["amount"],["cjbs"],["yclose"],["syl1"],["syl2"],["buy1"],["sale1"],["bc1"],["sc1"] from tradetable datekey %s+21/24 to %s+16/24 of '%s' end;b:=update a set ['date']=datetimetostr(['date']) end;return a;''' % (
        #     ts.LocalCallFunc("StrToDate", [BegDate])[1], ts.LocalCallFunc("StrToDate", [EndDate])[1], contract_code),
        # {}))[1])

        if self.mc is None:
            # self.mc = mongodb_client()
            self.mc.dbConnect()



        # dateList=self.getTradeDays(
        #     (datetime.datetime.strptime(BegDate, '%Y-%m-%d') - datetime.timedelta(3)).strftime('%Y-%m-%d'), EndDate)
        # BegDateAdj=dateList[max(0,dateList.index(BegDate)-1)]

        tick_dict = self.tsbytestostr(ts.RemoteExecute(
        '''a:=select ["StockID"],["StockName"],["date"],["price"],["vol"],["amount"],["cjbs"],["yclose"],["syl1"],["syl2"],["buy1"],["sale1"],["bc1"],["sc1"] from tradetable datekey %s to %s+0.9999999 of '%s' end;b:=update a set ['date']=datetimetostr(['date']) end;return a;''' % (
            ts.LocalCallFunc("StrToDate", [BegDate])[1], ts.LocalCallFunc("StrToDate", [EndDate])[1], contract_code),
        {}))[1]

        # datetime.datetime.strptime(tick_dict['date'][0], '%Y-%m-%d %H:%M:%S').replace(microsecond=500000)
        last_tick_datetime = None
        count = 0
        for d in tick_dict:
            tick = CtaTickData()
            tick.vtSymbol = d['StockName']

            tick.symbol = d['StockID']  # 合约代码   CF1705 StockID
            # tick.exchange = EMPTY_STRING  # 交易所代码

            # 成交数据
            tick.lastPrice = d['price']  # 最新成交价
            tick.volume = d['vol']   #最新成交量
            tick.amount = d['amount']  # 成交金额
            tick.cjbs = d['cjbs']  # 周期内成交笔数
            tick.yclose = d['yclose']  # 上一收盘价
            tick.preSettlement = d['syl2']  # 上一日结算价

            # tick的时间

            # 转换为datetime格式
            try:
                if len(d['date'])>10:
                    tick.datetime = datetime.datetime.strptime(d['date'], '%Y-%m-%d %H:%M:%S')  # python的datetime时间对象
                else:
                    tick.datetime = datetime.datetime.strptime(d['date'], '%Y-%m-%d')
                tick.date = tick.datetime.strftime('%Y-%m-%d')  # 日期
                tick.time = tick.datetime.strftime('%H:%M:%S')   # 时间
                # tick.tradingDay = d['price']  # 交易日期
            except Exception as ex:
                # 抛弃本tick
                print('日期转换错误:%s,error:%s' % (d['date'], ex))
                continue

            # 1档行情
            tick.bidPrice1 = d['buy1']
            tick.askPrice1 = d['sale1']
            tick.bidVolume1 = d['bc1']
            tick.askVolume1 = d['sc1']

            # 修正毫秒
            if tick.datetime.replace(microsecond=0) == last_tick_datetime:
                # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                tick.datetime = tick.datetime.replace(microsecond=500000)
                tick.time = tick.datetime.strftime('%H:%M:%S.%f')

            else:
                tick.datetime = tick.datetime.replace(microsecond=0)
                tick.time = tick.datetime.strftime('%H:%M:%S.%f')

            # 记录最新tick的时间
            last_tick_datetime = tick.datetime
            if symbol=='TC':
                symbol = 'ZC'
            if symbol == 'RO':
                symbol = 'OI'
            if symbol == 'ER':
                symbol = 'RI'
            if symbol == 'WS':
                symbol = 'WH'
            if symbol == 'ME':
                symbol = 'MA'

            self.mc.dbInsert('FUTURE_TICK_DB','TS_%s_TickDatas'%symbol,d=tick.__dict__)
            count = count + 1
        print('写入合约%s完成，共%d条'%(contract_code,count))
        return





if __name__ == '__main__':
    getDatas=tslFunctions()
    getDatas.tsl_login()

    BegDate='2018-12-28'
    EndDate='2019-01-05'
    # getDatas.getTickDetails(BegDate, BegDate, 'cu1902', re.findall("[A-Za-z]+",'cu1905')[0].upper())
    # #补周六
    # a = []
    # for day in pd.date_range('2013-07-05', '2018-12-17'):
    #     if day.weekday() == 5:
    #         a.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

    # codeList=getDatas.getDailyFuturesCode(BegDate, EndDate)
    dayList= getDatas.getTradeDays(BegDate, EndDate)

    #补周六
    for day in pd.date_range(BegDate, EndDate):
        if day.weekday() == 5:
            dayList.append(datetime.datetime.strftime(day, '%Y-%m-%d'))
    # dayList=EndDate
    from tqdm import tqdm
    tdqmDayList=tqdm(dayList)
    # tqdmList=tqdm(codeList)
    initialDB=True
    for day in tdqmDayList:
        codeList=getDatas.getDailyFuturesCode(day, day)
        tqdmCodeList = tqdm(codeList)
        if initialDB:
            listCode=list(set([re.findall("[A-Za-z]+", x)[0].upper() for x in codeList]))
            if getDatas.mc is None:
                getDatas.mc = mongodb_client()
                getDatas.mc.dbConnect()

            for code in listCode:
                getDatas.mc.dbDelete('FUTURE_TICK_DB','TS_%s_TickDatas'%code,{'date':{'$gte':BegDate,'$lte':EndDate}})
                print('del %s Done!'%code)
            initialDB=False


        for contract in tqdmCodeList:
            print("--------------------------------------------")
            print(day)
            getDatas.getTickDetails(day, day, contract,re.findall("[A-Za-z]+",contract)[0].upper())