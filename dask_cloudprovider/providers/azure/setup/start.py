import os
import sys
import uuid
import time
import socket
import argparse
import threading
import subprocess
import logging

from mpi4py import MPI
from azureml.core import Run
from notebook.notebookapp import list_running_servers

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    ip = socket.gethostbyname(socket.gethostname())

    run = Run.get_context()
    run.log("ip", ip)