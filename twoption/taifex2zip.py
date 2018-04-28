import os
import glob
import subprocess
import requests
from pyquery import PyQuery

OPT_ZIP_PATH = os.environ.get('OPT_ZIP_PATH', 'dataset/taifex_opt_tick_zip')
OPT_RPT_PATH = os.environ.get('OPT_RPT_PATH', 'dataset/taifex_opt_tick_rpt')

def get_taifex_opt_files():
    res = requests.get('http://www.taifex.com.tw/chinese/3/dl_3_2_4.asp')
    res.encoding = 'utf8'
    S = PyQuery(res.text)
    txf_op_tick_fnames = [PyQuery(btn).attr('onclick').split('/..')[-1].strip("')").split('/')[-1] for btn in S('#button7') if 'OptionsDailyDownload' in PyQuery(btn).attr('onclick')]
    return txf_op_tick_fnames

def _download_opt_tick(txf_op_tick_fname, op_tick_fpath):
    op_tick_url = f'http://www.taifex.com.tw/DailyDownload/OptionsDailyDownload/{txf_op_tick_fname}'
    res = requests.get(op_tick_url)
    with open(op_tick_fpath, 'wb') as f:
        f.write(res.content)
    print(txf_op_tick_fname)

def download_unexist_opt_files(txf_op_tick_fnames):
    exist_opt_zip = glob.glob(f'{OPT_ZIP_PATH}/*.zip')
    for txf_op_tick_fname in txf_op_tick_fnames:
        op_tick_fpath = f'{OPT_ZIP_PATH}/{txf_op_tick_fname}'
        if op_tick_fpath not in exist_opt_zip:
            _download_opt_tick(txf_op_tick_fname, op_tick_fpath)

def unzip_opt_tick_zip():
    exist_opt_zip = glob.glob(f'{OPT_ZIP_PATH}/*.zip')
    for zipfile in exist_opt_zip:
        subprocess.run(f'unzip -n {zipfile} -d {OPT_RPT_PATH}', shell=True)

        
if __name__ == '__main__':
    if not os.path.exists(OPT_ZIP_PATH):
        os.mkdir(OPT_ZIP_PATH)
    if not os.path.exists(OPT_RPT_PATH):
        os.mkdir(OPT_RPT_PATH)
    txf_op_tick_fnames = get_taifex_opt_files()
    download_unexist_opt_files(txf_op_tick_fnames)
    unzip_opt_tick_zip()
