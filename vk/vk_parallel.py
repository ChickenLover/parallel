import itertools
from multiprocessing import Process
import importlib
import time
import os
import sys

import requests
from requests_toolbelt.adapters import source

from vk_api8 import VKApi, AuthException
from db import initialize_mongo

bots_per_ip = 3
use_ips = 5
SCOPE = 'video'
ips = ['92.63.74.' + str(ip) for ip in range(135, 135 + use_ips)]
PROJ_NAME = 'fsb'
SCRIPT_NAME = 'load_videos_to_db'
THREADS_DELAY = 2
CUR_VER = "5.69"
USE_SPLIT = True
if USE_SPLIT:
    input_file_name = f'ids.txt'


def gen_session(ip):
        s = requests.Session()
        new_source = source.SourceAddressAdapter(ip)
        s.mount('http://', new_source)
        s.mount('https://', new_source)
        return s


def thread_wrapper(args, logfile, func):
    sys.stdout = logfile
    sys.stderr = logfile
    func(*args)


bots = itertools.cycle(list((initialize_mongo('data', 'data_bots').find({'status':0}))))
clients_cycle = itertools.cycle([client["id"] for client in\
                                                  initialize_mongo("spam", "clients").find()])
script = importlib.import_module("." + SCRIPT_NAME, package=PROJ_NAME)
os.chdir(PROJ_NAME)

apis = list()
for bot in bots:
    if len(apis) == use_ips * bots_per_ip: break
    try:
        apis.append(VKApi(bot['login'], bot['pass'], next(clients_cycle),
                          version=CUR_VER, scope=SCOPE))
    except AuthException as e:
        print(e)
        continue
if len(apis) < bots_per_ip*use_ips:
    print('NOT ENOUGH BOTS TO RUN WITH THOSE PARAMS')
    exit(1)
apis = iter(apis)

if USE_SPLIT:
    length = 0
    with open(input_file_name) as f:
        for line in f:
            length += 1
    per_thread = length // (use_ips * bots_per_ip)
    full_length = per_thread * (length // per_thread + 1)
    ARGS_ITERS = [iter(range(0, full_length, per_thread)),
                  iter(range(per_thread, full_length, per_thread))]
else:
    ARGS_ITERS = [iter(range(0, 5550, 917)), iter(range(917, 5550, 917))]


procs = list()
logs = list()
if not os.path.exists(f'logs'):
    os.makedirs(f'logs')
for i, ip in enumerate(ips):
    for j in range(bots_per_ip):
        api = next(apis)
        api.session = gen_session(ip)
        try:
            script_args = [str(next(arg_iter)) for arg_iter in ARGS_ITERS]
        except StopIteration:
            break
        args = [api, *script_args]
        print('*MASTER*: Opened script with args ', ' '.join(script_args), ' on ip ', ip)
        log = open(f"logs/{ip}_{j}.log", 'w', bufferring=0)
        logs.append(log)
        process = Process(target=thread_wrapper, args=(args, log, script.main), daemon=True)
        process.start()
        procs.append(process)

try:
    while procs:
        time.sleep(15)
        for proc, log in zip(procs, logs):
            log.flush()
            if not proc.is_alive:
                print(proc.exitcode)
                log.close()
                logs.remove(log)
                procs.remove(proc)
                
except KeyboardInterrupt:
    pass

for log in logs:
    log.close()
