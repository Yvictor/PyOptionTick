import os
import io
import glob
import tqdm
import pandas as pd

OPT_RPT_PATH = os.environ.get('OPT_RPT_PATH', 'dataset/taifex_opt_tick_rpt')
OPT_MSG_PATH = os.environ.get('OPT_MSG_PATH', 'dataset/taifex_opt_tick_msgpack')
OPT_H5_PATH = os.environ.get('OPT_H5_PATH', 'dataset/taifex_opt_tick_h5')

def _read_op_rpt(opt_rpt):
    with open(opt_rpt, 'r', encoding='big5') as f:
        op_data = f.read()
    op_data = [[r.strip(' ') for r in l.split(',')] for l in op_data.split('\n')]
    return op_data

def _op_data2df(op_data):
    df_op_tick = pd.DataFrame(op_data[2:], columns=op_data[0]).dropna()
    df_op_tick = pd.read_csv(io.StringIO(df_op_tick.iloc[:,:-1].to_csv()), 
                            index_col=[1], parse_dates={'成交日期時間' :['成交日期', '成交時間']}, 
                            converters={'到期月份(週別)': str})
    return df_op_tick

def filter_delivery_date(exec_date, df_op_tick):
    df_op_tick_exec = df_op_tick[df_op_tick['到期月份(週別)']==exec_date]
    opt_codes = df_op_tick_exec['code'].unique()
    for op_code in opt_codes:
        df_op_tick_exec_cp = df_op_tick_exec[df_op_tick_exec['code'] == op_code]
        df_op_tick_exec_cp.to_hdf(f'{OPT_H5_PATH}/exec_{exec_date}.h5', op_code, 
                                  format='t', complevel=5, complib='zlib')

def rpt2h5(opt_rpt, opt_name):
    op_data = _read_op_rpt(opt_rpt)
    df_op_tick = _op_data2df(op_data)
    df_op_tick.to_msgpack(f'{OPT_MSG_PATH}/{opt_name}.msgpack', encoding='utf-8', compress='zlib')
    df_op_tick = df_op_tick[df_op_tick['商品代號']=='TXO'].copy()
    df_op_tick['code'] = df_op_tick['買賣權別'] + df_op_tick['履約價格'].astype(int).astype(str)
    exec_dates = df_op_tick['到期月份(週別)'].unique()
    for exec_date in exec_dates:
        filter_delivery_date(exec_date, df_op_tick)

def all_rpt_opt2h5():
    opt_rpts = glob.glob(f'{OPT_RPT_PATH}/*.rpt')
    processed_opts = [opt.split('/')[-1].split('.')[0] for opt in glob.glob(f'{OPT_MSG_PATH}/*.msgpack')]
    for opt_rpt in tqdm.tqdm(opt_rpts[:]):
        opt_name = opt_rpt.split('/')[-1].strip('.rpt')
        if opt_name not in processed_opts:
            rpt2h5(opt_rpt, opt_name)

if __name__ =='__main__':
    if not os.path.exists(OPT_MSG_PATH):
        os.mkdir(OPT_MSG_PATH)
    if not os.path.exists(OPT_H5_PATH):
        os.mkdir(OPT_H5_PATH)
    
    all_rpt_opt2h5()