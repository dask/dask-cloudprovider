# Imports 
import os
import sys
import uuid
import time
import socket
import argparse
import threading
import subprocess

from mpi4py import MPI
from azureml.core import Run
from notebook.notebookapp import list_running_servers

def flush(proc, proc_log):
    while True:
        proc_out = proc.stdout.readline()
        if proc_out == '' and proc.poll() is not None:
            proc_log.close()
            break
        elif proc_out:
            sys.stdout.write(proc_out)
            proc_log.write(proc_out)
            proc_log.flush()

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    ### PARSE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler_ip_port", default=None)
    parser.add_argument("--use_gpu",           default=False)
    parser.add_argument("--n_gpus_per_node",   default=0)

    args, unparsed = parser.parse_known_args()
    
    ### CONFIGURE GPU RUN
    GPU_run = eval(args.use_gpu)

    if GPU_run:
        n_gpus_per_node = eval(args.n_gpus_per_node)

    attempt = 0
    ip = None

    while ip == None:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except socket.timeout:
            time.sleep(1)
            print(f'attempt: {attempt}, ip: {ip}')
            # pass
        attempt += 1
        
    print("- scheduler is ", args.scheduler_ip_port)
    print("- args: ", args)
    print("- unparsed: ", unparsed)
    print("- my rank is ", rank)
    print("- my ip is: ", ip)
    print("- n_gpus_per_node: ", n_gpus_per_node)
    
    if not GPU_run:
        cmd = "dask-worker " + args.scheduler_ip_port 
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(list(range(n_gpus_per_node))).strip("[]")
        cmd = "dask-cuda-worker " + args.scheduler_ip_port + " --memory-limit 0"
                
    worker_log = open("worker_{rank}_log.txt".format(rank=rank), "w")
    worker_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    flush(worker_proc, worker_log)