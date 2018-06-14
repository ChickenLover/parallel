import itertools
from multiprocessing import Process
import importlib
import time
import os
import sys


use_ips = 5
threads_per_ip = 5

ips = ['92.63.74.' + str(ip) for ip in range(135, 135 + use_ips)]
PROJ_NAME = "views"
SCRIPT_NAME = f'countviews.py'
THREADS_DELAY = 2

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

script = importlib.import_module("." + SCRIPT_NAME, package=PROJ_NAME)
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
    ARGS_ITERS = [iter(range(0, 5550, 917)), iter(range(917, 5550, 917))]

procs = list()
logs = list()
if not os.path.exists(f'logs'):
    os.makedirs(f'logs')
for i, ip in enumerate(ips):
    for j in range(threads_per_ip):
        session = gen_session(ip)
        try:
            script_args = [str(next(arg_iter)) for arg_iter in ARGS_ITERS]
        except:
            break
        args = [session, *script_args]
        print('*MASTER*: Opened script with args ', ' '.join(script_args), ' on ip ', ip, ' thread #', j)
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
            if not proc.is_alive:
                print(proc.exitcode)
                log.close()
                logs.remove(log)
                procs.remove(proc)
except KeyboardInterrupt:
    pass

for log in logs:
    log.close()
