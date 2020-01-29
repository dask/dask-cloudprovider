from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
from azureml.core.environment import CondaDependencies
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration

class AzureMLCluster:
    def __init__(self
        , workspace                     # AML workspace object
        , compute                       # AML compute object
        , node_count                    # initial node count, must be less than 
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
        self.workspace=workspace
        self.compute=compute
        self.node_count=node_count
        self.environment_name=environment_name
        self.experiment_name=experiment_name
        self.update_environment=update_environment
        self.docker_image=docker_image
        self.jupyter_port=jupyter_port
        self.dask_dashboard_port=dask_dashboard_port
        self.codefileshare=codefileshare
        self.datafileshare=self.workspace.get_default_datastore() if datafileshare == None else datafileshare
        self.use_GPU=use_GPU
        self.gpus_per_node=gpus_per_node
        self.use_existing_run=use_existing_run
        self.kwargs=kwargs
        self.workers_list=[]
        self.run=self.create_cluster()

        self.__print_message('Initiated')

    def __print_message(self, msg, length=80, filler='#', pre_post=''):
        print(f'{pre_post} {msg} {pre_post}'.center(length, filler))
        
    def create_cluster(self):
        # set up environment
        self.__print_message('Setting up environment')
        
        if (
               self.environment_name not in self.workspace.environments 
            or self.update_environment
        ):
            self.__print_message('Rebuilding')
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
                env.python.conda_dependencies=conda_dep

            env = env.register(self.workspace)
        else:
            env = self.workspace.environments[self.environment_name]

        script_params, env_params = {}, {}

        script_params['--jupyter'] = True
        script_params['--code_store'] = self.workspace.datastores[self.codefileshare]
        script_params['--data_store'] = self.workspace.datastores[self.datafileshare]

        if self.use_GPU:
            script_params['--use_GPU'] = True
            script_params['--n_gpus_per_node'] = self.gpus_per_node

        # submit run
        self.__print_message('Submitting the experiment')
        exp = Experiment(self.workspace, self.experiment_name)

        if self.use_existing_run==True: 
            run = next(exp.get_runs())
        else:
            est = Estimator(
                  'dask_cloudprovider/providers/azureml/setup'
                , compute_target=self.compute
                , entry_script='start_dask_cluster.py'
                , environment_definition=env
                , script_params=script_params
                , node_count=self.node_count
                , distributed_training=MpiConfiguration()
                , use_docker=True
                , **env_params
            )

            run = exp.submit(est)

        return run
    
    # def connect_cluster(self):
    #     if not self.run: 
    #         sys.exit("run doesn't exist.")
    #     dashboard_port=4242

    #     print("waiting for scheduler node's ip")
    #     while self.run.get_status()!='Canceled' and 'scheduler' not in self.run.get_metrics():
    #         print('.', end ="")
    #         time.sleep(5)
            
    #     print(self.run.get_metrics()["scheduler"])
        
    #     if self.run.get_status() == 'Canceled':
    #         print('\nRun was canceled')
    #     else:
    #         print(f'\nSetting up port forwarding...')
    #         os.system(f'killall socat') # kill all socat processes - cleans up previous port forward setups 
    #         os.system(f'setsid socat tcp-listen:{dashboard_port},reuseaddr,fork tcp:{self.run.get_metrics()["dashboard"]} &')
    #         print(f'Cluster is ready to use.')

    #     c = Client(f'tcp://{self.run.get_metrics()["scheduler"]}')
    #     print(f'\n\n{c}')

    #     #get the dashboard link 
    #     dashboard_url = f'https://{socket.gethostname()}-{dashboard_port}.{self.workspace.get_details()["location"]}.instances.azureml.net/status'
    #     HTML(f'<a href="{dashboard_url}">Dashboard link</a>')

    #     return c
    
    # def scale_up(self, workers=1):
    #     for i in range(workers):
    #         est = Estimator(
    #             'setup',
    #             compute_target=self.compute,
    #             entry_script='childRun.py', # pass scheduler ip from parent run
    #             environment_definition=self.workspace.environments[self.environment_name],
    #             script_params={'--datastore': self.workspace.get_default_datastore(), '--scheduler': self.run.get_metrics()["scheduler"]},
    #             node_count=1,
    #             distributed_training=MpiConfiguration()
    #         )

    #         child_run = Experiment(self.workspace, experiment_name).submit(est)
    #         self.workers_list.append(child_run)
            
    # #scale down
    # def scale_down(self, workers=1):
    #      for i in range(workers):
    #             if self.workers_list:
    #                 child_run=self.workers_list.pop(0) #deactive oldest workers
    #                 child_run.cancel()
    #             else:
    #                 print("All scaled workers are removed.")