# Imports
import os
import sys
import time
import socket
import argparse
import subprocess
import logging
from mpi4py import MPI
from azureml.core import Run


def flush(proc, proc_log):
    while True:
        proc_out = proc.stdout.readline()
        if proc_out == "" and proc.poll() is not None:
            proc_log.close()
            break
        elif proc_out:
            sys.stdout.write(proc_out)
            proc_log.write(proc_out)
            proc_log.flush()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    ### PARSE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler_ip_port", default=None)
    parser.add_argument("--worker_death_timeout", default=30)  # 30 seconds
    parser.add_argument("--use_gpu", default=False)
    parser.add_argument("--n_gpus_per_node", default=0)

    args, unparsed = parser.parse_known_args()

    ### CONFIGURE GPU RUN
    GPU_run = eval(args.use_gpu)
    n_gpus_per_node = eval(args.n_gpus_per_node)

    attempt = 0
    ip = None

    run = Run.get_context()

    while ip is None:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except (socket.timeout, socket.gaierror):
            time.sleep(1)
            logger.debug(f"attempt: {attempt}, ip: {ip}")
            # pass
        attempt += 1

    logger.debug("- scheduler is ", args.scheduler_ip_port)
    logger.debug("- args: ", args)
    logger.debug("- unparsed: ", unparsed)
    logger.debug("- my rank is ", rank)
    logger.debug("- my ip is: ", ip)

    if not GPU_run:
        cmd = "dask-worker " + args.scheduler_ip_port
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(list(range(n_gpus_per_node))).strip(
            "[]"
        )
        cmd = (
            "dask-cuda-worker "
            + args.scheduler_ip_port
            + " --memory-limit 0 "
            + " --death-timeout "
            + args.worker_death_timeout
        )

    worker_log = open("worker_{rank}_log.txt".format(rank=rank), "w")
    worker_proc = subprocess.Popen(
        cmd.split(),
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    flush(worker_proc, worker_log)
