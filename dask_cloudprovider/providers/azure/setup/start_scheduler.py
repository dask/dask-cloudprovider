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
    parser.add_argument("--jupyter",         default=False)
    parser.add_argument("--datastores",      default=[])  # datastore default value cannot be None because of enumeration later
    parser.add_argument("--jupyter_token",   default=uuid.uuid1().hex)
    parser.add_argument("--jupyter_port",    default=8888)
    parser.add_argument("--dashboard_port",  default=8787)
    parser.add_argument("--scheduler_port",  default=8786)
    parser.add_argument("--use_gpu",         default=False)
    parser.add_argument("--n_gpus_per_node", default=0)
    # parser.add_argument("--script")

    args, unparsed = parser.parse_known_args()
    
    ### CONFIGURE GPU RUN
    GPU_run = args.use_gpu

    if GPU_run:
        n_gpus_per_node = eval(args.n_gpus_per_node)

    ip = socket.gethostbyname(socket.gethostname())
    
    ### SETUP THE HEADNODE
    if rank == 0:
        data = {
            "scheduler"  : ip + ':' + str(args.scheduler_port),
            "dashboard"  : ip + ':' + str(args.dashboard_port),
            "jupyter"    : ip + ':' + str(args.jupyter_port),
            "token"      : args.jupyter_token,
            "datastores" : args.datastores
            # "datastore"  : args.data_store
            }
    else:
        data = None
    
    ### DISTRIBUTE TO CLUSTER
    data = comm.bcast(data, root=0)
    scheduler  = data["scheduler"]
    dashboard  = data["dashboard"]
    jupyter    = data["jupyter"]
    datastores = data['datastores']
    # codestore = data["codestore"]
    # datastore = data["datastore"]
    token      = data["token"]

    print("- scheduler is ", scheduler)
    print("- dashboard is ", dashboard)
    print("- args: ", args)
    print("- unparsed: ", unparsed)
    print("- my rank is ", rank)
    print("- my ip is ", ip)
    
    if rank == 0:
        running_jupyter_servers = list(list_running_servers())
        print(running_jupyter_servers)

        ### if any jupyter processes running
        ### KILL'EM ALL!!!
        if len(running_jupyter_servers) > 0: 
            for server in running_jupyter_servers:
                os.system(f'kill {server["pid"]}')

        ### RECORD LOGS
        Run.get_context().log('headnode', ip)
        Run.get_context().log('scheduler', scheduler) 
        Run.get_context().log('dashboard', dashboard)
        Run.get_context().log('jupyter', jupyter)
        Run.get_context().log('token', token)
        for i, datastore in enumerate(datastores): # TODO: mount all datastors 
            Run.get_context().log(f'datastore_{i}', datastore)
        # Run.get_context().log('codestore', codestore)
        # Run.get_context().log('datastore', datastore)
        
        if args.jupyter:
            cmd = (f' jupyter lab --ip 0.0.0.0 --port {args.jupyter_port}' + \
                              f' --NotebookApp.token={token}'              + \
                            #   f' --notebook-dir={codestore}/..'            + \
                              f' --allow-root --no-browser')
    
            jupyter_log = open("jupyter_log.txt", "a")
            jupyter_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
            jupyter_flush = threading.Thread(target=flush, args=(jupyter_proc, jupyter_log))
            jupyter_flush.start()
    
            while not list(list_running_servers()):
                time.sleep(5)
    
            jupyter_servers = list(list_running_servers())
            assert (len(jupyter_servers) == 1), "more than one jupyter server is running"

        cmd = "dask-scheduler " + "--port " + scheduler.split(":")[1] + " --dashboard-address " + dashboard
        scheduler_log = open("scheduler_log.txt", "w")
        scheduler_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        ### CHOOSE THE WORKER TYPE TO RUN ON HEADNODE
        if not GPU_run:
            cmd = "dask-worker " + scheduler 
        else:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(list(range(n_gpus_per_node))).strip("[]")
            cmd = "dask-cuda-worker " + scheduler + " --memory-limit 0"

        worker_log = open("worker_{rank}_log.txt".format(rank=rank), "w")
        worker_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        worker_flush = threading.Thread(target=flush, args=(worker_proc, worker_log))
        worker_flush.start()

        flush(scheduler_proc, scheduler_log)
