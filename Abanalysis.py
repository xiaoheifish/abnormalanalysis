"""
异常订单检测
日期：2018-2-26
By：Yu Bihan
"""

import pymysql
import pandas as pd
from datetime import datetime
from datetime import timedelta
import requests
import time
import sched
import traceback


# 获取当前时间
def get_current_time():
    current = datetime.now()
    return current.strftime("%Y-%m-%d %H:%M:%S")


# 计算一天的时间区间
def compute_date_interval():
    dtoday = datetime.now()
    dtomorrow = dtoday + timedelta(days=1)
    return dtoday.strftime("%Y-%m-%d"), dtomorrow.strftime("%Y-%m-%d")


# 将计算出的相邻两笔订单的时间差转为秒
def time_to_seconds(t):
    return timedelta.total_seconds(t)


# 取出时间差在10秒以内的全部订单
def compute_diff_time(df):
    df['time'] = pd.to_datetime(df['gmtcreate'])
    df['pre'] = df['time'].shift(1)
    df['difftime'] = df['time'] - df['pre']
    dfnew1 = df.dropna()
    if dfnew1.empty:
        return dfnew1
    dfnew1['difftime'] = dfnew1['difftime'].apply(time_to_seconds)
    dfintense = dfnew1[dfnew1['difftime'] <= 10]
    dfintense['pregid'] = dfintense['gid'].shift(1)
    dfintense['diffgid'] = dfintense['gid'] - dfintense['pregid']
    dfintense.drop(['pregid'], axis=1, inplace = True)
    return dfintense.dropna()


# 弥补diffgid的误差
def add_one(x):
    if x == -1:
        return -1
    elif x != 0:
        return x + 1
    else:
        return 0


# 弥补difftime的误差
def add_one_change_two(x):
    if x > 0:
        return x + 1
    elif x == -1:
        return 2
    else:
        return 0


# 如果断线时间和密集交易时间在10秒以内，则不判断为虚假交易，但要发短信提示
def get_recent_disconnect(intenseTime, connRecord):
    for record in connRecord:
        if (intenseTime - record[0]).seconds <= 10:
            return True
    return False


class Abanalysis:
    def __init__(self, max_total_income = 25, cycle = 24):
        # 正常订单数最大值
        self.MAX_TOTAL_INCOME = max_total_income
        # 检测周期
        self.cycle = cycle
        # 短信通知url
        self.message_url = "http://www.terabits-wx.cn/site/pythonmessage"
        # 邮件通知url
        self.email_url = "http://www.terabits-wx.cn/site/pythonemail"

        # 日志文件路径初始化
        self.debug_path = 'log/normal.log'
        self.error_path = 'log/error.log'

        # 正常日志标记
        self.debug_log = 1
        # 异常日志标记
        self.error_log = 2

        # 数据库基本信息
        self.ip = "119.23.210.52"
        self.port = 3312
        self.user = "tb"
        self.passwd = "tb123456"
        self.db = "chargingpile"

        # 频繁断线设备
        self.frequent_disconnect_device = []
        # 密集交易设备
        self.frequent_trading_device = []

    # 取回某一时间段消费记录，用于后续分析
    def get_all_record(self, deviceid, begintime, endtime):
        db = pymysql.connect(host=self.ip, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
        cursor = db.cursor()
        sql = "select * from t_charge_consumerecord where deviceid = %(deviceid)s and gmtcreate >= %(begintime)s and gmtcreate <= %(endtime)s"
        value = {"deviceid": deviceid, "begintime": begintime, "endtime": endtime}
        cursor.execute(sql,value)
        data = cursor.fetchall()
        db.close()
        df = pd.DataFrame([[ij for ij in i] for i in data])
        df.rename(columns={0:'gid',1:'consumeorderno',2:'openid',3:'phone',4:'imei',5:'deviceid',6:'sitename',7:'adminname',8:'city',9:'payment',10:'freepayment',11:'prebalance',12:'postbalance',
                       13:'type',14:'orderid',15:'status',16:'gmtcreate',17:'gmtmodified'}, inplace=True);
        if df.empty:
            return df
        dfnew = df[['gid','deviceid','imei','sitename','payment','gmtcreate']]
        return dfnew

    # 获得订单总金额
    def get_total_income(self, df):
        return df.sum()['payment'] < self.MAX_TOTAL_INCOME

    # 查询断线情况，若无断线，返回False；若有断线，返回断线时间
    def get_connection_record(self, imei, begintime, endtime):
        db = pymysql.connect(host=self.ip, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
        cursor = db.cursor()
        getDeviceSql = "select gmtcreate from t_charge_connectionrecord where status = 1 and imei = %(imei)s and gmtcreate >= %(begintime)s and gmtcreate <= %(endtime)s"
        param = {"imei": imei, "begintime": begintime, "endtime": endtime}
        cursor.execute(getDeviceSql, param)
        data = cursor.fetchall()
        db.close()
        if not data:
            return False
        else:
            return data

    # 计算difftime小于10的订单，gid间隔在1，2，3之内的连续订单数
    def compute_continue_number(self, df):
        diffgid = list(df['diffgid'])
        record = []
        for number in range(len(diffgid)):
            record.append(0)
        gidsum = 0
        nextindex = 0
        for number in range(len(diffgid)):
            while(number < len(diffgid) and (diffgid[number] == 1.0 or diffgid[number] == 2.0 or diffgid[number] == 3.0)):
                number = number + 1
                gidsum = gidsum + 1
            if number > nextindex:
                record[number - gidsum] = gidsum
                nextindex = number
            gidsum = 0
        ser = pd.Series(record, index = df.index)
        df['continue'] = ser
        df['preContinue'] = df['continue'].shift(-1)
        df['preContinue'] = df['preContinue'].apply(add_one)
        df.drop(['continue'], axis=1, inplace = True)

    # 合并并删除多余的行
    def merge_and_delete(self, df, dftmp):
        dfpart = dftmp[['gid','preContinue']]
        dfmerge = pd.merge(df, dfpart, how = 'left', on = ['gid'])
        dfmerge.drop(['gmtcreate', 'pre', 'difftime'], axis=1, inplace = True)
        return dfmerge

    # 计算gid间隔时，每个密集交易段的第一笔订单没有计算在内，这个函数用于把这笔订单算进去
    def add_first_order(self, dfmerge):
        dfmerge['finalcon'] = dfmerge['preContinue'].shift(-1)
        dfmerge['finalcon'] = dfmerge['finalcon'].apply(add_one_change_two)
        dfmerge.drop(['preContinue'], axis=1, inplace=True)


    # 将由于断线导致的密集交易和真实的密集交易区分开
    def classify_abnormal_trading(self, df, connRecord):
        dfcon = df[df['finalcon'] > 3]
        disconnection = []
        abnormaltrading = []
        for index, row in dfcon.iterrows():
            if get_recent_disconnect(row['time'], connRecord):
                disconnection.append(row['gid'])
            else:
                abnormaltrading.append(row['gid'])
        return disconnection, abnormaltrading

    # 无断线记录时用此方法
    def get_abnormal_trading(self, df):
        dfcon = df[df['finalcon'] > 3]
        abnormaltrading = []
        for index, row in dfcon.iterrows():
            abnormaltrading.append(row['gid'])
        return abnormaltrading

    # 挑选出待标记的密集交易
    def get_gid_mark(self, dfmerge, abnormaltrading):
        removelist = []
        for gid in abnormaltrading:
            index = dfmerge.loc[dfmerge['gid'] == gid].index
            continueValue = dfmerge.iloc[index]['finalcon']
            lastIndex = index + int(continueValue)
            dftmp = dfmerge.iloc[index[0]:lastIndex[0]]
            for shortgid in list(dftmp['gid']):
                removelist.append([int(shortgid)])
        return removelist

    # 批量更新数据库
    def update_abnormal_trading(self, removelist):
        db = pymysql.connect(host=self.ip, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
        cursor = db.cursor()
        try:
            updateSql = "update t_charge_consumerecord set fake = 1 where gid = %s"
            # 执行SQL语句
            cursor.executemany(updateSql, removelist)
            # 提交到数据库执行
            db.commit()
            self.write_log(self.debug_log, '以下订单被标记为异常订单' + str(removelist))
        except Exception as e:
            # 发生错误时回滚
            db.rollback()
            self.write_log(self.error_log, '标记异常订单时发生错误' + str(removelist))
            #exstr = traceback.format_exc()
            #print(exstr)
        # 关闭数据库连接
        db.close()

    # 短信通知管理员，通过java后台通知
    def send_message(self, disconlist, abnormallist):
        param = {'disconnection' : disconlist, 'abnormaltrading' : abnormallist}
        try:
            reply = requests.post(self.message_url, param, timeout = 0.01)
        except:
            self.write_log(self.error_log, "短信通知失败，无法连接服务器。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
            return
        if reply.status_code == 200:
            reply.encoding = 'utf-8'
            reply = reply.json()
            if reply['result'] == 'ok':
                self.write_log(self.debug_log, "短信通知成功，断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
            if reply['result'] == 'error':
                self.write_log(self.error_log, "短信通知失败，服务器内部问题。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
        else:
            self.write_log(self.error_log, "短信通知失败，未收到回复。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))

    # 邮件通知管理员，通过java后台通知
    def send_email(self, disconlist, abnormallist):
        conn = ','.join(str(e) for e in disconlist)
        trade = ','.join(str(e) for e in abnormallist)
        param = {'disconnection':conn, 'abnormaltrading':trade, 'time':self.cycle}
        try:
            reply = requests.post(self.email_url, param, timeout = 0.01)
        except:
            self.write_log(self.error_log, "邮件通知失败，无法连接服务器。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
            return
        if reply.status_code == 200:
            reply.encoding = 'utf-8'
            reply = reply.json()
            if reply['result'] == 'ok':
                self.write_log(self.debug_log, "邮件通知成功，断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
            if reply['result'] == 'error':
                self.write_log(self.error_log, "邮件通知失败，服务器内部问题。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))
        else:
            self.write_log(self.error_log, "邮件通知失败，未收到回复。断线设备" + str(disconlist) + "异常设备" + str(abnormallist))

    # 记录日志
    def write_log(self, type, content):
        path = self.debug_path
        if type == self.error_log:
            path = self.error_path
        with open(path, 'a') as log:
            log.write("currenttime:" + get_current_time())
            log.write(content)
            log.write('\n')
            log.close()

    def single_device_analysis(self, deviceid):
        # 获得时间区间
        begintime, endtime = compute_date_interval()
        #begintime, endtime = '2018-02-26', '2018-02-27'
        pri_record = self.get_all_record(deviceid, begintime, endtime)
        # 如果当天没有订单
        if pri_record.empty:
            return False
        # 如果订单总数小于允许值
        if self.get_total_income(pri_record):
            return False
        # 计算时间差
        diff_record = compute_diff_time(pri_record)
        if diff_record.empty:
            return False
        # 计算连续订单数
        self.compute_continue_number(diff_record)
        merge_record = self.merge_and_delete(pri_record, diff_record)
        self.add_first_order(merge_record)
        # 获取连接情况
        connection = self.get_connection_record(merge_record.iloc[0]['imei'], begintime, endtime)
        disconnection = []
        abnormal_trading = []
        # 如果没有断线
        if not connection:
            abnormal_trading = self.get_abnormal_trading(merge_record)
        if connection:
            disconnection, abnormal_trading = self.classify_abnormal_trading(merge_record, connection)
        # 断线表里加入此设备
        if disconnection:
            self.frequent_disconnect_device.append(deviceid)
        # 密集交易表里加入此设备，修改数据库里的标记
        if abnormal_trading:
            self.frequent_trading_device.append(deviceid)
            marked_gid = self.get_gid_mark(merge_record, abnormal_trading)
            self.update_abnormal_trading(marked_gid)

    def run(self):
        for deviceid in range(100001, 100463):
            print(deviceid)
            self.single_device_analysis(deviceid)
        self.send_email(self.frequent_disconnect_device, self.frequent_trading_device)
        self.send_message(self.frequent_disconnect_device, self.frequent_trading_device)

# 定时任务
def perform_task():
    scheduler.enter(60*60*24, 0, perform_task)
    detector = Abanalysis(max_total_income=25, cycle=24)
    detector.run()

if __name__ == "__main__":
    scheduler = sched.scheduler(time.time, time.sleep)
    now = datetime.now()
    sched_time = datetime(now.year, now.month, now.day, 23, 59, 0)
    scheduler.enterabs(sched_time.timestamp(), 0, perform_task)  # datetime.timestamp()是python
    scheduler.run()
    #detector = Abanalysis(max_total_income=25)
    #detector.run()
