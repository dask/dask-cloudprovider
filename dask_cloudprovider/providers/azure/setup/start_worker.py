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
    parser.add_argument("--datastores",        default=[])
    parser.add_argument("--scheduler_ip_port", default=None)
    parser.add_argument("--use_gpu",           default=False)
    parser.add_argument("--n_gpus_per_node",   default=0)

    args, unparsed = parser.parse_known_args()
    dict={x.split("=")[0]: x.split("=")[1] for x in unparsed}

    print("dict is ", dict)
    scheduler_ip=dict['scheduler_ip_port']
    
    ### CONFIGURE GPU RUN
    GPU_run = True if dict['use_gpu'] == 'True' else False
    if GPU_run:
        n_gpus_per_node = eval(dict['n_gpus_per_node'])

    node_ip = socket.gethostbyname(socket.gethostname())
    
    print("- scheduler ip: ", scheduler_ip)
    print("- Current node ip: ", node_ip)
    print("- args: ", args)
    print("- unparsed: ", unparsed)
    print("- my rank is ", rank)

    if not GPU_run:
        cmd = "dask-worker " + scheduler_ip 
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(list(range(n_gpus_per_node))).strip("[]")
        cmd = "dask-cuda-worker " + scheduler_ip + " --memory-limit 0"
                
    worker_log = open("worker_{rank}_log.txt".format(rank=rank), "w")
    worker_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    flush(worker_proc, worker_log)
