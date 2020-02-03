import asyncio
from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
from azureml.core.environment import CondaDependencies
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration
from IPython.core.display import HTML
import time, os, socket, sys

from distributed.deploy.cluster import Cluster
from distributed.core import rpc

class AzureMLCluster(Cluster):
    def __init__(self
        , workspace                     # AML workspace object
        , compute_target                # AML compute object
        , initial_node_count = 1        # initial node count, must be less than
                                        # or equal to AML compute object max nodes
                                        # will default to max nodes if more than
        , experiment_name='DaskAML'     # name of the experiment to start
        , use_existing_run=False        # use existing run if any
        , environment_definition=None   # name of the environment to use
        , pip_packages=None             # list of pip packages to install
        , conda_packages=None           # list of conda packages to install 
        , use_gpu=False                 # flag to indicate GPU vs CPU cluster
        , n_gpus_per_node=None          # number of GPUs per node if use_GPU flag set
        , docker_image=None             # optional -- docker image
        , jupyter=True                  # start Jupyter lab process on headnode
        , jupyter_port=9000             # port to forward the Jupyter process to
        , dashboard_port=9001           # port to forward Dask dashboard to
        , datastores=[]                 # datastores specs
        , asynchronous=False            # flag to run jobs in an asynchronous way
        , **kwargs
    ):
        ### GENERAL FLAGS
        self.workspace = workspace
        self.compute_target = compute_target
        self.initial_node_count = initial_node_count

        ### EXPERIMENT DEFINITION
        self.experiment_name = experiment_name
        self.use_existing_run = use_existing_run

        ### ENVIRONMENT AND VARIABLES
        self.environment_definition = environment_definition
        self.pip_packages = pip_packages 
        self.conda_packages = conda_packages
        
        ### GPU RUN INFO
        self.use_gpu = use_gpu
        self.n_gpus_per_node = n_gpus_per_node
        
        ### JUPYTER AND PORT FORWARDING
        self.jupyter = jupyter
        self.jupyter_port = jupyter_port
        self.dashboard_port = dashboard_port

        ### DATASTORES
        self.datastores = datastores
        
        ### FUTURE EXTENSIONS
        self.kwargs = kwargs

        ### PARAMETERS TO START THE CLUSTER
        self.scheduler_params = {}
        self.worker_params = {}
 
        ### CLUSTER PARAMS
        self.max_nodes = (
            self
            .compute_target
            .serialize()
            ['properties']
            ['properties']
            ['scaleSettings']
            ['maxNodeCount']
        )
        self.scheduler_ip_port = None
        self.workers_list = []
        self.URLs = {}
        # self.status = "created"     ### REQUIRED BY distributed.deploy.cluster.Cluster

        ### SANITY CHECKS
        ###-----> initial node count
        if self.initial_node_count > self.max_nodes:
            self.initial_node_count = self.max_nodes

        ###-----> environment spec
        if self.environment_definition and (
               (type(self.pip_packages)   is list and len(self.pip_packages)   > 0)
            or (type(self.conda_packages) is list and len(self.conda_packages) > 0)
        ):
            
            raise Exception('Specify only `environment_definition` or either `pip_packages` or `conda_packages`.')

        # ### INITIALIZE CLUSTER
        # self.create_cluster()
        super().__init__(asynchronous=asynchronous)

    def __print_message(self, msg, length=80, filler='#', pre_post=''):
        print(f'{pre_post} {msg} {pre_post}'.center(length, filler))

    def __get_estimator(self):
        return None

    # async def _start(self):
    #     self.__print_message('Starting cluster...')

    def create_cluster(self):
        # set up environment
        self.__print_message('Setting up cluster')

        ### scheduler and worker parameters
        self.scheduler_params['--jupyter'] = True
        # self.scheduler_params['--code_store'] = self.workspace.datastores[self.codefileshare]
        # self.scheduler_params['--data_store'] = self.workspace.datastores[self.datafileshare]
        
        ### ADD DATASTORES
        temp_datastores = []
        for datastore in self.datastores:
            temp_datastores.append(self.workspace.datastores[datastore])            
        self.datastores = temp_datastores

        self.scheduler_params['--datastores'] = [['sss', 'fff']]

        # self.scheduler_params['--datastores'] = self.datastores
        # self.worker_params['--datastores']    = self.datastores
        # # self.worker_params['--code_store'] = self.workspace.datastores[self.codefileshare]
        # # self.worker_params['--data_store'] = self.workspace.datastores[self.datafileshare]

        if self.use_gpu:
            self.scheduler_params['--use_gpu'] = True
            self.scheduler_params['--n_gpus_per_node'] = self.n_gpus_per_node
            self.worker_params['--use_gpu'] = True
            self.worker_params['--n_gpus_per_node'] = self.n_gpus_per_node

        # submit run
        self.__print_message('Submitting the experiment')
        exp = Experiment(self.workspace, self.experiment_name)

        run = None
        if self.use_existing_run == True:
            runs = exp.get_runs()

            try:
                run = next(x for x in runs if ('scheduler' in x.get_metrics() and x.get_status() == 'Running'))
            except StopIteration:
                run = None
                self.__print_message('NO EXISTING RUN WITH SCHEDULER', filler='!')
        
        if not run:
            est = Estimator(
                  'dask_cloudprovider/providers/azureml/setup'
                , compute_target=self.compute_target
                , entry_script='start_scheduler.py'
                , environment_definition=self.environment_definition
                , script_params=self.scheduler_params
                , node_count=1 ### start only scheduler
                , distributed_training=MpiConfiguration()
                , use_docker=True
            )

            run = exp.submit(est)

            self.__print_message("Waiting for scheduler node's ip")
            while (
                (
                       run.get_status() != 'Canceled' 
                    or run.get_status() != 'Failed'
                )
                and 'scheduler' not in run.get_metrics()
            ):
                print('.', end="")
                time.sleep(5)
            
            print('\n\n')
            
            self.scheduler_ip_port = run.get_metrics()["scheduler"]
            self.worker_params['--scheduler_ip_port'] = self.scheduler_ip_port
            self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')
            
            ### REQUIRED BY dask.distributed.deploy.cluster.Cluster
            self.scheduler_comm = rpc(run.get_metrics()["scheduler"])     
            super()._start()

        self.run = run
        # self.scale(self.initial_node_count - 1)

        self.__print_message(self.status)

        ### TESTING ONLY
        self.run.cancel()
        self.run.complete()

    def connect_cluster(self):
        if not self.run:
            sys.exit("Run doesn't exist!")

        dashboard_port = self.dask_dashboard_port

        if self.run.get_status() == 'Canceled':
            print('\nRun was canceled')
        else:
            print(f'\nSetting up port forwarding...')
            self.port_forwarding_ComputeVM()
            self.print_links_ComputeVM()
            print(f'Cluster is ready to use.')

    def port_forwarding_ComputeVM(self):
        os.system(f'killall socat') # kill all socat processes - cleans up previous port forward setups
        os.system(f'setsid socat tcp-listen:{self.dask_dashboard_port},reuseaddr,fork tcp:{self.run.get_metrics()["dashboard"]} &')
        os.system(f'setsid socat tcp-listen:{self.jupyter_port},reuseaddr,fork tcp:{self.run.get_metrics()["jupyter"]} &')

    def print_links_ComputeVM(self):
        #get the dashboard link
        dashboard_url = f'https://{socket.gethostname()}-{self.dask_dashboard_port}.{self.workspace.get_details()["location"]}.instances.azureml.net/status'
        self.__print_message(f'DASHBOARD: {dashboard_url}')
        self.URLs['dashboard'] = HTML(f'<a href="{dashboard_url}">Dashboard link</a>')

        # build the jupyter link
        jupyter_url = f'https://{socket.gethostname()}-{self.jupyter_port}.{self.workspace.get_details()["location"]}.instances.azureml.net/lab?token={self.run.get_metrics()["token"]}'
        self.__print_message(f'NOTEBOOK: {jupyter_url}')
        self.URLs['notebook'] =  HTML(f'<a href="{jupyter_url}">Jupyter link</a>')

    def get_links(self):
        return self.URLs

    def scale(self, workers=1):
        count=len(self.workers_list)

        if count < workers:
            self.scale_up(workers-count)
        elif count > workers:
            self.scale_down(count-workers)
        else:
            print(f'Number of workers: {workers}')

    # scale up
    def scale_up(self, workers=1):
        for i in range(workers):
            est = Estimator(
                 'dask_cloudprovider/providers/azureml/setup'
                , compute_target=self.compute_target
                , entry_script='start_worker.py' # pass scheduler ip from parent run
                , environment_definition=self.environment_definition
                , script_params=self.worker_params
                , node_count=1
                , distributed_training=MpiConfiguration()
            )

            child_run = Experiment(self.workspace, self.experiment_name).submit(est)
            self.workers_list.append(child_run)

    # scale down
    def scale_down(self, workers=1):
         for i in range(workers):
                if self.workers_list:
                    child_run=self.workers_list.pop(0) #deactive oldest workers
                    child_run.cancel()
                else:
                    print("All scaled workers are removed.")
                    
    # close cluster
    def close(self):
        while self.workers_list:
            child_run=self.workers_list.pop()
            child_run.cancel()
            child_run.complete()
        if self.run:
            self.run.cancel()
            self.run.complete()
        self.__print_message("Scheduler and workers are disconnected.")
