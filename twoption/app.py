import os
from pathlib import Path
import numpy as np
import pandas as pd
import h5py
from flask import Flask, send_from_directory, jsonify, request, session

FLASK_SECRETKEY = os.environ.get("FLASK_SECRETKEY", "")
STATIC_FOLDER_PATH = os.environ.get("STATIC_FOLDER_PATH", "static")
DEBUG = os.environ.get("DEBUG", "TRUE").lower() == "true"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 5000))
dataset_h5_path = Path('dataset/tick_h5')


app = Flask(__name__, static_url_path='', static_folder=STATIC_FOLDER_PATH)
app.secret_key = FLASK_SECRETKEY


@app.route('/')
def index():
    return send_from_directory(STATIC_FOLDER_PATH, "index.html")


@app.route('/<path:path>')
def static_folder(path):
    pass


@app.route('/<guid>')
def index_redirect(guid):
    return send_from_directory(STATIC_FOLDER_PATH, 'index.html')


api_version = 'v1'


@app.route(f'/api/{api_version}/config', methods=['GET'])
def config():
    return jsonify({'exchanges': [{'value': 'TAIFEX', 'name': 'TAIFEX', 'desc': '台灣期貨交易所'}],
                    "supports_search": True, "supports_group_request": False,
                    'supported_resolutions': ['1', '3', '5', '15', '30', '60', '1D'],
                    'supports_marks': False,
                    "symbols_types": [{"name": "All", "value": ""}, {"name": "Option", "value": "option"}, ],
                    })


@app.route(f'/api/{api_version}/symbol_info', methods=['GET'])
def symbol_info():
    group = request.args.get('group')

    if group == 'TAIFEX':
        return jsonify({'symbol': ['TXO_201306_C7750', ],
                        'description': ['TXO', ],
                        'exchange-listed': 'TAIFEX',
                        'exchange-traded': 'TAIFEX',
                        "minmovement": 1,
                        "minmovement2": 0,
                        "pricescale": [10, ],
                        "has-dwm": True,
                        "has-intraday": True,
                        "has-no-volume": [False, ],
                        "type": ["option", ],
                        "ticker": ['TXO_201306_C7750', ],
                        "timezone": 'Etc/UTC',
                        "session-regular": "0845-1345",
                        })


@app.route(f'/api/{api_version}/search', methods=['GET'])
def search():
    query = request.args.get('query')
    type_ = request.args.get('type')
    exchange = request.args.get('exchange')
    limit = int(request.args.get('limit'))
    groups = ["TXO_{}".format(p.stem.strip('exec_'))
              for p in dataset_h5_path.rglob('*h5')]
    h5p = None
    rt_list = [{"symbol": p,
                "full_name": p,
                "description": "Option",
                "exchange": "TAIFEX",
                "ticker": p,
                "type": "option",
                } for p in groups if query in p]
    if query[:10] in groups and 'W' not in query:
        h5p = dataset_h5_path.joinpath(
            "{}.h5".format(query[:10].replace('TXO', 'exec')))
    if query[:12] in groups and query[:12].count('_') < 2:
        h5p = dataset_h5_path.joinpath(
            "{}.h5".format(query[:12].replace('TXO', 'exec')))
    if h5p:
        with h5py.File(str(h5p), mode='r') as h5f:
            symbols = [("TXO_{}_{}".format(h5p.stem.strip('exec_'), k),
                        h5f[k]['table'].size) for k in h5f.keys()]
        rt_list = [{"symbol": p,
                    "full_name": p,
                    "description": "tick: {}".format(size),
                    "exchange": "TAIFEX",
                    "ticker": p,
                    "type": "option",
                    } for p, size in symbols if query in p]
    return jsonify(rt_list[:limit])


@app.route(f'/api/{api_version}/symbols', methods=['GET'])
def symbols():
    symbol = request.args.get('symbol')
    name, exec_date, strike_price = symbol.split('_')
    return jsonify({'name': symbol, 'type': 'option',
                    'session': '0845-1345', 'exchange-traded': 'TWS', 'timezone': 'Etc/UTC',

                    'minmov': 1, 'minmov2': 0, 'pricescale': 10, 'has_intraday': True,
                    'supported_resolutions':  ['1', '3', '5', '15', '30', '60', '1D'], })


@app.route(f'/api/{api_version}/history', methods=['GET'])
def history():
    symbol = request.args.get('symbol')
    from_ = request.args.get('from')
    to_ = request.args.get('to')
    resolution = request.args.get('resolution', ' ')
    if not resolution[-1].isalpha():
        resolution = "{}T".format(resolution)
    name, exec_date, strike_price = symbol.split('_')
    from_d, to_d = pd.to_datetime([from_, to_], unit='s')
    symbol_path = dataset_h5_path.joinpath('exec_{}.h5'.format(exec_date))
    df_tick = pd.read_hdf(str(symbol_path), strike_price).set_index("成交日期時間")
    df_tick_selected = df_tick[from_d:to_d].copy()
    resampler = df_tick_selected.resample(resolution, base=15)
    df_resample = resampler.agg(
        {'成交價格': ['first', 'max', 'min', 'last'], "成交數量(B or S)": 'sum'})
    df_resample.columns = df_resample.columns.droplevel()
    df_resample.rename(columns={'first': 'open', 'max': 'high',
                                'min': 'low', 'last': 'close', 'sum': 'vol'}, inplace=True)
    df_resample.index.name = 'datetime'
    df_resample = df_resample.between_time('08:45', '13:45')
    df_resample.fillna(method='ffill', inplace=True)#.dropna(inplace=True)  #
    if len(df_resample):
        rt_dict = {'t': (df_resample.index.astype(np.int64).values // 10**9).tolist(),
                   'c': df_resample['close'].tolist(),
                   'o': df_resample['open'].tolist(),
                   'h': df_resample['high'].tolist(),
                   'l': df_resample['low'].tolist(),
                   'v': df_resample['vol'].tolist(),
                   's': 'ok', }
    elif len(df_tick) and len(df_tick[:from_d]):
        rt_dict = {'s': 'no_data',
                   'nextTime': (df_tick[:from_d].iloc[-2:-1].index.astype(np.int64).values // 10**9).tolist()[0]}
    else:
        rt_dict = {'s': 'no_data',
                   }

    return jsonify(rt_dict)


if __name__ == '__main__':
    app.run(debug=DEBUG, host=HOST, port=PORT)
