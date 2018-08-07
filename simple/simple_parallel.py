import itertools
from multiprocessing import Process
import importlib
import time
import os
import sys

import requests
from requests_toolbelt.adapters import source


use_ips = 5
threads_per_ip = 3

ips = ['92.63.74.' + str(ip) for ip in range(135, 135 + use_ips)]
PROJ_NAME = "ctf"
SCRIPT_NAME = f'countviews'
THREADS_DELAY = 5

USE_SPLIT = False
if USE_SPLIT:
    input_file_name = f'alumni.csv'


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

script = importlib.import_module('.' + SCRIPT_NAME, package=PROJ_NAME)
os.chdir(PROJ_NAME)

if USE_SPLIT:
    length = 0
    with open(input_file_name) as f:
        for line in f:
            length += 1
    per_thread = length // (use_ips * threads_per_ip)
    full_length = per_thread * (length // per_thread + 1)
    ARGS_ITERS = [iter(range(0, full_length, per_thread)),
                  iter(range(per_thread, full_length, per_thread))]
else:
    ARGS_ITERS = [iter([106350, 124075, 141800, 159525, 177250, 194975]), iter([124075, 141800, 159525, 177250, 194975, 212700])]

procs = list()
logs = list()
if not os.path.exists(f'logs'):
    os.makedirs(f'logs')
for i, ip in enumerate(ips):
    for j in range(threads_per_ip):
        session = gen_session(ip)
        try:
            script_args = [next(arg_iter) for arg_iter in ARGS_ITERS]
        except:
            break
        args = [session, *script_args]
        print('*MASTER*: Opened script with args ',\
              ' '.join(str(arg) for arg in script_args),\
              ' on ip ', ip, ' thread #', j)
        log = open(f"logs/{ip}_{j}.log", 'w')
        logs.append(log)
        process = Process(target=thread_wrapper, args=(args, log, script.main), daemon=True)
        process.start()
        procs.append(process)
        time.sleep(THREADS_DELAY)

try:
    while procs:
        time.sleep(15)
        for proc, log in zip(procs, logs):
            log.flush()
            if proc.exitcode is not None:
                print(proc.exitcode)
                log.close()
                logs.remove(log)
                procs.remove(proc)
except KeyboardInterrupt:
    pass

for log in logs:
    log.close()
