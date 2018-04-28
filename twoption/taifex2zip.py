import os
import glob
import requests
from pyquery import PyQuery

OPT_ZIP_PATH = os.environ.get('OPT_ZIP_PATH', 'dataset/taifex_opt_tick_zip')

def get_taifex_opt_files():
    res = requests.get('http://www.taifex.com.tw/chinese/3/dl_3_2_4.asp')
    res.encoding = 'utf8'
    S = PyQuery(res.text)
    txf_op_tick_fnames = [PyQuery(btn).attr('onclick').split('/..')[-1].strip("')").split('/')[-1] for btn in S('#button7') if 'OptionsDailyDownloadCSV' in PyQuery(btn).attr('onclick')]
    return txf_op_tick_fnames

if __name__ == '__main__':
    txf_op_tick_fnames = get_taifex_opt_files()
    exist_opt_zip = glob.glob('dataset/taifex_opt_tick_zip/*.zip')
    for txf_op_tick_fname in txf_op_tick_fnames:
        op_tick_fpath = f'{OPT_ZIP_PATH}/{txf_op_tick_fname}'
        if op_tick_fpath not in exist_opt_zip:
            print(txf_op_tick_fname)
            op_tick_url = f'http://www.taifex.com.tw/DailyDownload/OptionsDailyDownloadCSV/{txf_op_tick_fname}'
            res = requests.get(op_tick_url)
            with open(op_tick_fpath, 'wb') as f:
                f.write(res.content)