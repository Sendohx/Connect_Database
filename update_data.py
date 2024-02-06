
import os
import pandas as pd

from datetime import datetime, timedelta
from connect_database import ConnectDatabase

class DataUpdater(ConnectDatabase):
    def __init__(self, sql, root, asset, start_date):
        super().__init__(sql)
        self.root = root
        self.asset = asset
        self.start_date = start_date
        self.end_date = datetime.today().strftime("%Y%m%d")

    def update_ind_data(self):
        files = os.listdir(self.root)
        for file in files:
            if file.startswith(f"ind_{asset}"):
                file_path = os.path.join(self.root, file)
                os.remove(file_path)
        data = super().get_data()
        data = data.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_PRECLOSE': 'pre_close',
                                    'S_DQ_OPEN': 'open', 'S_DQ_HIGH': 'high', 'S_DQ_LOW': 'low', 'S_DQ_CLOSE': 'close',
                                    'S_DQ_AMOUNT': 'amount'})
        data[data.columns[2:]] = (data[data.columns[2:]].apply(pd.to_numeric))
        data.sort_values(['symbol', 'date'], inplace=True)
        data['return'] = data['close'] / data['pre_close'] - 1
        data.to_parquet(self.root + f'/ind_{asset}_{self.start_date}_{self.end_date}.parquet')
        print(f'{asset} ind data updated')

    def update_stk_data(self):
        files = os.listdir(self.root)
        for file in files:
            if file.startswith(f"stk_{asset}"):
                file_path = os.path.join(self.root, file)
                os.remove(file_path)
        stk_data = super().get_data()
        stk_data = stk_data.rename(columns={'S_INFO_WINDCODE': 'symbol', 'TRADE_DT': 'date', 'S_DQ_MV': 'capital',
                                            'FREE_SHARES_TODAY': 'free_share'})
        stk_data[stk_data.columns[2:]] = stk_data[stk_data.columns[2:]].apply(pd.to_numeric, errors='coerce')
        stk_data.sort_values(['symbol', 'date'], inplace=True)
        stk_data.to_parquet(self.root + f'/stk_{self.asset}_{self.start_date}_{self.end_date}.parquet')
        print(f'{asset} stk data updated')

if __name__ == '__main__':
    root = '/nas92/xujiahao/data/raw'
    start_date = '20130101'
    end_date = datetime.now().strftime('%Y%m%d')
    data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=700)
    data_start_date = data_start_date.strftime('%Y%m%d')
    assets = ['000985.CSI', '000300.SH', '000852.SH', '932000.CSI', '000905.SH']

    # 指数数据
    temp_dict = dict()
    for asset in assets:
        table = 'AINDEXEODPRICES'
        columns = 'S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_AMOUNT'
        condition1 = f"S_INFO_WINDCODE = '{asset}'"
        condition2 = f"TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}'"
        sql = f''' SELECT %s FROM %s WHERE %s AND %s ''' % (columns, table, condition1, condition2)
        du = DataUpdater(sql, root, asset, start_date)
        du.update_ind_data()
    """
    # 股票数据
    for asset in assets:
        sql = f'''
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
        du = DataUpdater(sql, root, asset, start_date)
        du.update_stk_data()
    """
    print('done')