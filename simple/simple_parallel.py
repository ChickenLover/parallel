import itertools
import subprocess
import time
import os


use_ips = 5
threads_per_ip = 5

ips = ['92.63.74.' + str(ip) for ip in range(135, 135 + use_ips)]
PROJ_NAME = "views"
SCRIPT_NAME = f'countviews.py'
THREADS_DELAY = 2

os.chdir(PROJ_NAME)

USE_SPLIT = True
if USE_SPLIT:
    input_file_name = f'alumni/alumni.csv'
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
        command = ["python3.6", SCRIPT_NAME]
        try:
            script_args = [str(next(arg_iter)) for arg_iter in ARGS_ITERS]
        except:
            continue
        args = [ip, *script_args]
        print('*MASTER*: Opened script with args ', ' '.join(script_args), ' on ip ', ip, ' thread #', j)
        log = open(f"logs/{ip}_{j}.log", 'w')
        logs.append(log)
        procs.append(subprocess.Popen(command + args, stdout=log, stderr=log))
        time.sleep(THREADS_DELAY)

try:
    while True:
        if not procs:
            break
        time.sleep(15)
        for proc, log in zip(procs, logs):
            status = proc.poll()
            log.flush()
            if status: 
                print(status)
                procs.remove(proc)
                logs.remove(log)
except KeyboardInterrupt:
    pass

for proc, log in zip(procs, logs):
    proc.kill()
    log.close()
