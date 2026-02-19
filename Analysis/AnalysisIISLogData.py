import pandas as pd
import glob
import os
import sys
from datetime import datetime
import pytz

# 参考URL: https://chikuwamarux.hatenablog.com/entry/20210920/1632107640

#
# フォルダにアルファイルを全て読み込み DataFrame に登録する
#
def loadDirFiles(path):
    df = pd.DataFrame(columns = [])
    for filename in glob.glob(path + "/*"):
        print('読み込み中... ' + filename)
        tmp = pd.read_csv(filename, encoding="cp932")
        df = pd.concat([df, tmp])
    return df
#
#
#
def parse_datetime(x):
    dt = datetime.strptime(x[1:-7], '%d/%b/%Y:%H:%M:%S')
    dt_tz = int(x[-6:-3])*60+int(x[-3:-1])
    return dt.replace(tzinfo=pytz.FixedOffset(dt_tz))

#
#
#
def read_IIS_log( filename ):
    #フィールド名の設定 
    log_field_names = ['date', 'time', 's-sitename', 's-computernam','s-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 's-port', 'cs-username', 'c-ip', 'cs(User-Agent)','cs(Cookie)', 'cs(Referer)','cs-host', 'sc-status', 'sc-substatus', 'sc-win32-status','sc-bytes', 'cs-bytes', 'time-taken']
    iis_log = pd.read_csv( filename, sep=' ', comment='#',  names=log_field_names)
    iis_log['date'] = pd.to_datetime(iis_log ['date'])                                     
    iis_log['time'] = pd.to_datetime(iis_log ['time'])
    return iis_log

#
#
#
def read_Apache_log( filename ):
    apache_log = pd.read_csv(
        filename,
        sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])',
        na_values='-',
        header=None,
        usecols=[0, 3, 4, 5, 6],
        names=['ip', 'time', 'request', 'status', 'size'],
        converters={'time': parse_datetime,
                    'status': int,
                    'size': int})
    return apache_log

#
# メイン関数
#
if __name__ == '__main__':
    args = sys.argv
    if len(args) >= 3:
        if os.path.isdir(args[1]):
            # 引数の取得
            userlist = args[2]
            df = pd.DataFrame(columns = [])
            df = loadDirFiles(args[1])
            if len(df) == 0:
                print('読み込んだデータは1件もありませんでした。')                
            else:
                # 引数で指定されたユーザリストをカンマ区切りでユーザIDを取り出し
                # その取り出したユーザID毎にデータを抽出してCSVファイルに出力する
                for userId in userlist.split(','):
                    result = pd.DataFrame(columns=[])
                    result = searchData(df,userId)
                    result.to_csv(userId + '.csv', encoding='cp932', mode='w', index=False)
        else:
            print('フォルダ' + args[1] + 'が存在しません。')
    else:
        print('パラメータ数が足りません。')
        print('python AnalysisLogData.py <ログファイルがあるフォルダ> <抽出したいユーザリスト>')
        print(' ex: python AnalysisLogData.py c:\temp\logdata ID90000,ID90001')
    