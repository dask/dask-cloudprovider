from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
from azureml.core.environment import CondaDependencies
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration
from IPython.core.display import HTML
import time, os, socket, sys

class AzureMLCluster:
    def __init__(self
        , workspace                     # AML workspace object
        , compute                       # AML compute object
        , initial_node_count            # initial node count, must be less than
                                        # or equal to AML compute object max nodes
        , environment_name              # name of the environment to use
        , experiment_name               # name of the experiment to start
        , docker_image=None             # optional -- docker image
        , jupyter_port=9000
        , dask_dashboard_port=9001
        , codefileshare=None
        , datafileshare=None
        , update_environment=False
        , use_GPU=False
        , gpus_per_node=None
        , use_existing_run=False
        , **kwargs
    ):
        self.workspace = workspace
        self.compute = compute
        self.initial_node_count = initial_node_count
        self.environment_name = environment_name
        self.environment_obj = None
        self.experiment_name = experiment_name
        self.update_environment = update_environment
        self.docker_image = docker_image
        self.jupyter_port = jupyter_port
        self.dask_dashboard_port = dask_dashboard_port
        self.codefileshare = codefileshare
        self.datafileshare = self.workspace.get_default_datastore() if datafileshare == None else datafileshare
        self.use_GPU = use_GPU
        self.gpus_per_node = gpus_per_node
        self.use_existing_run = use_existing_run
        self.kwargs = kwargs
        self.max_nodes = self.compute.serialize()['properties']['properties']['scaleSettings']['maxNodeCount']
  
        self.scheduler_params = {}
        self.worker_params = {}
 
        ### CLUSTER PARAMS
        self.scheduler_ip_port = None
        self.workers_list = []
        self.URLs = {}

        ### SANITY CHECKS
        if self.initial_node_count > self.max_nodes:
            raise Exception(f'Initial count of nodes ({self.initial_node_count}) greater than allowed: {self.max_nodes}')

        self.create_cluster()

        # self.__print_message('Initiated')

    def __print_message(self, msg, length=80, filler='#', pre_post=''):
        print(f'{pre_post} {msg} {pre_post}'.center(length, filler))

    def create_cluster(self):
        # set up environment
        self.__print_message('Setting up environment')

        if (
               self.environment_name not in self.workspace.environments
            or self.update_environment
        ):
            self.__print_message('Rebuilding environment')
            env = Environment(name=self.environment_name)
            env.docker.enabled = True
            env.docker.base_image = self.docker_image

            if self.use_GPU and 'python_interpreter' in self.kwargs:
                env.python.interpreter_path = self.kwargs['python_interpreter']
                env.python.user_managed_dependencies = True

            ### CHECK IF pip_packages or conda_packages in kwargs
            conda_dep = None
            if 'pip_packages' in self.kwargs or 'conda_packages' in self.kwargs:
                conda_dep = CondaDependencies()

            if 'pip_packages' in self.kwargs:
                if self.kwargs['pip_packages'] is list:
                    for pip_package in self.kwargs['pip_packages']:
                        conda_dep.add_pip_package(pip_package)

            if 'conda_packages' in self.kwargs:
                if self.kwargs['pip_packages'] is list:
                    for conda_package in self.kwargs['conda_packages']:
                        conda_dep.add_conda_package(conda_package)

            if conda_dep is not None:
                env.python.conda_dependencies = conda_dep

            self.environment_obj = env.register(self.workspace)
        else:
            self.environment_obj = self.workspace.environments[self.environment_name]

        ### scheduler and worker parameters
        self.scheduler_params['--jupyter'] = True
        self.scheduler_params['--code_store'] = self.workspace.datastores[self.codefileshare]
        self.scheduler_params['--data_store'] = self.workspace.datastores[self.datafileshare]

        self.worker_params['--code_store'] = self.workspace.datastores[self.codefileshare]
        self.worker_params['--data_store'] = self.workspace.datastores[self.datafileshare]

        if self.use_GPU:
            self.worker_params['--use_GPU'] = True
            self.worker_params['--n_gpus_per_node'] = self.gpus_per_node

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
                , compute_target=self.compute
                , entry_script='start_scheduler.py'
                , environment_definition=self.environment_obj
                , script_params=self.scheduler_params
                , node_count=1 ### start only scheduler
                , distributed_training=MpiConfiguration()
                , use_docker=True
            )

            run = exp.submit(est)

            self.__print_message("Waiting for scheduler node's ip")
            while (
                run.get_status() != 'Canceled' 
                and 'scheduler' not in run.get_metrics()
            ):
                print('.', end="")
                time.sleep(5)
            
            print('\n\n')
            self.scheduler_ip_port = run.get_metrics()["scheduler"]
            self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')

        self.run = run
        self.scale(self.initial_node_count - 1)

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
                , compute_target=self.compute
                , entry_script='start_worker.py' # pass scheduler ip from parent run
                , environment_definition=self.environment_obj
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
                    self.__print_message("All scaled workers are removed.")
                    
    # close cluster
    def close(self):
        while self.workers_list:
            child_run=self.workers_list.pop()
            child_run.cancel()
        if self.run:
            self.run.cancel()
        self.__print_message("Scheduler and workers are disconnected.")
