
import pandas as pd

from datetime import datetime, timedelta
from connect_database import ConnectDatabase

root = '/nas92/xujiahao/data/raw'
start_date = '20130101'
end_date = '20240112'
data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=700)
data_start_date = data_start_date.strftime('%Y%m%d')
assets = ['000985.CSI', '000300.SH', '000852.SH', '932000.CSI']

# 指数及成分股日行情数据
temp_dict = dict()
for asset in assets:
    table = 'AINDEXEODPRICES'
    columns = 'S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_AMOUNT'
    condition1 = f"S_INFO_WINDCODE = '{asset}'"
    condition2 = f"TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}'"

    sql1 = f''' SELECT %s FROM %s WHERE %s AND %s ''' % (columns, table, condition1, condition2)
    cd1 = ConnectDatabase(sql1)
    data = cd1.get_data()
    data = data.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_PRECLOSE': 'pre_close',
                                'S_DQ_OPEN': 'open', 'S_DQ_HIGH': 'high', 'S_DQ_LOW': 'low', 'S_DQ_CLOSE': 'close',
                                'S_DQ_AMOUNT': 'amount'})
    data[data.columns[2:]] = (data[data.columns[2:]].apply(pd.to_numeric))
    data = data.sort_values(['symbol', 'date']).copy()
    data['return'] = data['close']/data['pre_close'] - 1

    sql2 = f'''
                SELECT A.S_INFO_WINDCODE, A.TRADE_DT, A.S_DQ_MV, A.FREE_SHARES_TODAY
                FROM ASHAREEODDERIVATIVEINDICATOR A
                WHERE A.TRADE_DT > '{data_start_date}' AND A.S_INFO_WINDCODE IN (
                    SELECT B.S_CON_WINDCODE
                    FROM AINDEXMEMBERS B
                    WHERE B.S_INFO_WINDCODE = '{asset}'
                        AND (
                            (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE > A.TRADE_DT)
                        OR (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE IS NULL)
                        )
                    )
                '''
    cd2 = ConnectDatabase(sql2)
    stk_data = cd2.get_data()
    stk_data = stk_data.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_MV': 'capital',
                                        'FREE_SHARES_TODAY': 'free_share'})
    stk_data[stk_data.columns[2:]] = stk_data[stk_data.columns[2:]].apply(pd.to_numeric, errors='coerce')
    stk_data.sort_values(['symbol', 'date'], inplace=True)
    temp_dict[asset] = [data, stk_data]
    data.to_parquet(root + f'/ind_{asset}_{start_date}_{end_date}.parquet')
    stk_data.to_parquet(root + f'/stk_{asset}_{start_date}_{end_date}.parquet')
