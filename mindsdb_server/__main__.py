import subprocess
import os
import argparse
import importlib
import atexit
import time
from threading import Thread
from mindsdb_server.utilities.loop import register
from mindsdb_server.utilities import config


def die_gracefully(proc_arr):
    for p in proc_arr:
        p.kill()

parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
parser.add_argument('--api', type=str, default='http,mysql') # alternative when mysql api is ready: default='http,mysql'
parser.add_argument('--config', type=str, default='/etc/mindsdb/config.json')

args = parser.parse_args()

api_arr = args.api.split(',')

# placeholder <-- move config getting to utils
config.merge(args.config)


cdir = os.path.dirname(os.path.realpath(__file__))
proc_arr = []

supported_apis = ['http','mysql']
for api in api_arr:
    if api not in supported_apis:
        raise Exception(f'The {api} is not part of the supported apis: {supported_apis}')
    try:
        import random
        freq = random.randint(1,10)
        register(freq, print,('The controller has launched me as a peridoic process.', f'I run every {freq} seconds and am completely useless'))

        p = subprocess.Popen([config['python_interpreter'], f'{cdir}/api/{api}/start.py', f'--config={args.config}'])
        print(f'Started Mindsdb {api} API!')
        proc_arr.append(p)
    except Exception as e:
        print(f'Failed to start {api} API with exception: \n\n"{e}"\n\n')
        #exit()

print('Everything running !')
while True:
    for p in proc_arr:
        if p.poll() is not None:
            exit()
    time.sleep(2)

atexit.register(die_gracefully, proc_arr=proc_arr)

'''
import argparse
import importlib
import atexit
from multiprocessing import Pool, Process
import os


print(f'Main call under name {__name__}')

try:
    with open('infinite_loop_stop.txt','r') as fp:
        txt = fp.read()
except:
    txt = ''

if 'STOP' not in txt:

    def close_api_gracefully(pool):
        print('Shutting down !')
        pool.terminate()
        pool.join()

    with open('infinite_loop_stop.txt','w') as fp:
        fp.write('STOP')

    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default='http,mysql') # alternative when mysql api is ready: default='http,mysql'
    parser.add_argument('--config', type=str, default='/etc/mindsdb/config.json')

    args = parser.parse_args()
    api_arr = args.api.split(',')
    pool = Pool(processes=len(api_arr))

    p_arr = []

    for api in api_arr:
        print(api_arr)
        print(f'\n\n\n{api}\n\n\n')
        print(f'Starting Mindsdb {api} API !')
        try:
            start = importlib.import_module(f'mindsdb_server.api.{api}.start')
            p = Process(target=start.start)
            p.start()
            p_arr.append(p)
            print(f'Started Mindsdb {api} API ! <clap, clap, clap>')
        except Exception as e:
            #close_api_gracefully(pool)
            print(f'Failed to start {api} API with exception {e}')
            #exit()

    for p in p_arr:
        print(p)
        import time
        time.sleep(40)
        p.join()

    atexit.register(close_api_gracefully, pool=pool)
'''
