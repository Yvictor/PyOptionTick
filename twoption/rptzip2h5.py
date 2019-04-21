import os
import zipfile
import pandas as pd
import glob
import multiprocessing as mp
import tqdm
OPT_RPT_ZIP_PATH = os.environ.get('OPT_RPT_ZIP_PATH', 'dataset/opt_zip')
OPT_MSG_PATH = os.environ.get('OPT_MSG_PATH', 'dataset/opt_tick_msgpack')
OPT_H5_PATH = os.environ.get('OPT_H5_PATH', 'dataset/opt_tick_h5')

def _read_op_rpt_zip(opt_rpt):
    with zipfile.ZipFile(opt_rpt, mode='r') as zipf:
        op_data = zipf.read(zipf.namelist()[0]).decode('big5')
    op_data_line = [l for l in op_data.split('\r\n')]
    header = [r.strip(' ') for r in op_data_line[0].replace('交易日期', 
                                                             '成交日期').replace('交割年月', 
                                                                            '到期月份(週別)').replace('成交數量(B+S)', 
                                                                                                '成交數量(B or S)').split(',')]
    op_data = [[r.strip(' ') for r in l.split(',')] for l in op_data_line[2:]]
    return header, op_data

def _op_data2df(op_data, header):
    df_op_tick = pd.DataFrame(op_data, columns=header).dropna()
    df_op_tick['成交日期時間'] = pd.to_datetime(df_op_tick['成交日期'] + df_op_tick['成交時間'], format='%Y%m%d%H%M%S%f')
    df_op_tick = df_op_tick.drop(columns=['成交日期', '成交時間'])
    df_op_tick['履約價格'] = df_op_tick['履約價格'].astype(float)
    df_op_tick['成交價格'] = df_op_tick['成交價格'].astype(float)
    df_op_tick['成交數量(B or S)'] = df_op_tick['成交數量(B or S)'].astype(int)
    if '開盤集合競價' in df_op_tick.columns:
        df_op_tick = df_op_tick.drop('開盤集合競價', axis=1) 
    return df_op_tick


def filter_delivery_date(data):
    df_op_tick, exec_date = data
    df_op_tick_exec = df_op_tick[df_op_tick['到期月份(週別)']==exec_date]
    opt_codes = df_op_tick_exec['code'].unique()
    for op_code in opt_codes:
        df_op_tick_exec_cp = df_op_tick_exec[df_op_tick_exec['code'] == op_code]
        df_op_tick_exec_cp.to_hdf(f'{OPT_H5_PATH}/exec_{exec_date}.h5', op_code, mode='a',
                                    format='t', append=True, complevel=5, complib='zlib')
        
def rpt2h5(opt_rpt, opt_name):
    header, op_data = _read_op_rpt_zip(opt_rpt)
    df_op_tick = _op_data2df(op_data, header)
    df_op_tick.to_msgpack(f'{OPT_MSG_PATH}/{opt_name}.msgpack', encoding='utf-8', compress='zlib')
    df_op_tick = df_op_tick[df_op_tick['商品代號']=='TXO'].copy()
    df_op_tick['code'] = df_op_tick['買賣權別'] + df_op_tick['履約價格'].astype(int).astype(str)
    exec_dates = df_op_tick['到期月份(週別)'].unique()
    for exec_date in exec_dates:
        filter_delivery_date((df_op_tick, exec_date))
    #with mp.Pool(len(exec_dates)) as pool:
    #    pool.imap_unordered(filter_delivery_date, ((df_op_tick, exec_date) for exec_date in exec_dates))

def all_rpt_opt2h5():
    opt_rpt_zips = sorted(glob.glob(f'{OPT_RPT_ZIP_PATH}/*.zip'))
    processed_opts = [opt.split('/')[-1].split('.')[0] for opt in glob.glob(f'{OPT_MSG_PATH}/*.msgpack')]
    for opt_rpt in tqdm.tqdm(opt_rpt_zips[:]):
        opt_name = opt_rpt.split('/')[-1].strip('.zip')
        if opt_name not in processed_opts:
            print(opt_rpt)
            rpt2h5(opt_rpt, opt_name)        


if __name__ =='__main__':
    if not os.path.exists(OPT_MSG_PATH):
        os.mkdir(OPT_MSG_PATH)
    if not os.path.exists(OPT_H5_PATH):
        os.mkdir(OPT_H5_PATH)
    
    all_rpt_opt2h5()
