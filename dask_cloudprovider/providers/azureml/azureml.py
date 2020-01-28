from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment

class AzureMLCluster:
    def __init__(self
        , workspace
        , compute
        , node_count
        , environment_name
        , experiment_name
        , use_existing_run=False
    ):
        self.workspace=workspace
        self.compute=compute
        self.node_count=node_count
        self.environment_name=environment_name
        self.experiment_name=experiment_name
        self.use_existing_run=use_existing_run
        self.workers_list=[]
        self.run=self.create_cluster()

        print('Initiated...')
        
    # def create_cluster(self):        
    #     #set up environment
    #     if self.environment_name not in self.workspace.environments:
    #         env=Environment.from_existing_conda_environment(self.environment_name, 'azureml_py36')
    #         env.python.conda_dependencies.add_pip_package('mpi4py')
    #         env = env.register(ws)
    #     else:
    #         env = self.workspace.environments[self.environment_name]

    #     #submit run
    #     exp = Experiment(self.workspace, self.experiment_name)
    #     if self.use_existing_run==True: 
    #         run = next(exp.get_runs())
    #     else:
    #         est = Estimator(
    #             'setup',
    #             compute_target=self.compute,
    #             entry_script='start.py',
    #             environment_definition=env,
    #             script_params={'--datastore': self.workspace.get_default_datastore()},
    #             node_count=self.node_count,
    #             distributed_training=MpiConfiguration()
    #         )
    #         run = exp.submit(est)

    #     return run
    
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