import asyncio
import logging
# from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
# from azureml.core.environment import CondaDependencies
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration
# from IPython.core.display import HTML
import time, os, socket, sys

from distributed.deploy.cluster import Cluster
from distributed.core import rpc

from distributed.utils import (
    PeriodicCallback,
    log_errors,
    ignoring,
    sync,
    Log,
    Logs,
    thread_state,
    format_dashboard_link,
    format_bytes
)

class AzureMLCluster(Cluster):
    def __init__(self
        , workspace                     # AML workspace object
        , compute_target                # AML compute object
        , environment_definition        # AML Environment object
        , initial_node_count = 1        # initial node count, must be less than
                                        # or equal to AML compute object max nodes
                                        # will default to max nodes if more than
        , experiment_name='DaskAML'     # name of the experiment to start
        , use_existing_run=False        # use existing run if any
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
        
        ### GPU RUN INFO
        self.use_gpu = use_gpu
        self.n_gpus_per_node = n_gpus_per_node

        ### JUPYTER AND PORT FORWARDING
        self.jupyter = jupyter
        self.jupyter_port = jupyter_port
        self.dashboard_port = dashboard_port

        ### FUTURE EXTENSIONS
        self.kwargs = kwargs

        ### PARAMETERS TO START THE CLUSTER
        self.scheduler_params = {}
        self.worker_params = {}
 
        ### DATASTORES
        self.datastores = datastores
        
        ### scheduler and worker parameters
        self.scheduler_params['--jupyter'] = True    
        if self.use_gpu:
            self.scheduler_params['--use_gpu'] = True
            self.scheduler_params['--n_gpus_per_node'] = self.n_gpus_per_node
            self.worker_params['--use_gpu'] = True
            self.worker_params['--n_gpus_per_node'] = self.n_gpus_per_node

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

        ### SANITY CHECKS
        ###-----> initial node count
        if self.initial_node_count > self.max_nodes:
            self.initial_node_count = self.max_nodes

        ### INITIALIZE CLUSTER
        super().__init__(asynchronous=asynchronous)

        ### BREAKING CHANGE IN ASYNCIO API
        version_info = sys.version_info
        self.loop = asyncio.get_event_loop()
    
        if not self.loop.is_running():
            if version_info.major == 3:
                if version_info.minor < 7:
                    self.loop.run_until_complete(self.create_cluster())
                else:
                    asyncio.run(self.create_cluster())
            else:
                raise Exception('Python 3 required.')
        else:
            print('Attaching to existing loop...')
            asyncio.ensure_future(self.create_cluster())

    def __print_message(self, msg, length=80, filler='#', pre_post=''):
        print(f'{pre_post} {msg} {pre_post}'.center(length, filler))

    def __get_estimator(self):
        return None

    async def create_cluster(self):
        # set up environment
        self.__print_message('Setting up cluster')

        # submit run
        self.__print_message('Submitting the experiment')
        exp = Experiment(self.workspace, self.experiment_name)
        estimator = Estimator(
            'dask_cloudprovider/providers/azureml/setup'
            , compute_target=self.compute_target
            , entry_script='start_scheduler.py'
            , environment_definition=self.environment_definition
            , script_params=self.scheduler_params
            , node_count=1 ### start only scheduler
            , distributed_training=MpiConfiguration()
            , use_docker=True
        )

        run = exp.submit(estimator)

        self.__print_message("Waiting for scheduler node's IP")
        while (
            (run.get_status() != 'Canceled' 
            or run.get_status() != 'Failed') 
            and 'scheduler' not in run.get_metrics()):
            print('.', end="")
            time.sleep(5)

        print('\n\n')
            
        self.scheduler_ip_port = run.get_metrics()["scheduler"]
        self.worker_params['--scheduler_ip_port'] = self.scheduler_ip_port
        self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')
        self.run = run
        
        ### REQUIRED BY dask.distributed.deploy.cluster.Cluster
        self.scheduler_comm = rpc(run.get_metrics()["scheduler"])
        asyncio.ensure_future(super()._start())
        
        self.__print_message(f'Scaling to {self.initial_node_count} workers')
        if self.initial_node_count>1:
            self.scale(self.initial_node_count - 1)
        self.__print_message(f'Scaling is done')

    def connect_cluster(self):
        if not self.run:
            sys.exit("Run doesn't exist!")
        if self.run.get_status() == 'Canceled':
            print('\nRun was canceled')
        else:
            print(f'\nSetting up port forwarding...')
            self.port_forwarding_ComputeVM()
            print(f'Cluster is ready to use.')

    def port_forwarding_ComputeVM(self):
        os.system(f'killall socat') # kill all socat processes - cleans up previous port forward setups
        os.system(f'setsid socat tcp-listen:{self.dashboard_port},reuseaddr,fork tcp:{self.run.get_metrics()["dashboard"]} &')
        os.system(f'setsid socat tcp-listen:{self.jupyter_port},reuseaddr,fork tcp:{self.run.get_metrics()["jupyter"]} &')
        
        self.scheduler_info['dashboard_url'] = f'https://{socket.gethostname()}-{self.dashboard_port}.{self.workspace.get_details()["location"]}.instances.azureml.net/status'
        self.scheduler_info['jupyter_url'] = (
            f'https://{socket.gethostname()}-{self.jupyter_port}.{self.workspace.get_details()["location"]}.instances.azureml.net/lab?token={self.run.get_metrics()["token"]}'
        )
        
    @property
    def dashboard_link(self):
        try:
            link = self.scheduler_info['dashboard_url']
        except KeyError:
            return ""
        else:
            return link

    @property
    def jupyter_link(self):
        try:
            link = self.scheduler_info['jupyter_url']
        except KeyError:
            return ""
        else:
            return link
        
    def _format_workers(self, workers, requested, use_gpu, n_gpus_per_node=None):
        if use_gpu:
            if workers == requested:
                return f'{workers}'
            else:
                return f'{workers} / {requested}'

        else:
            if workers == requested:
                return f'{workers}'
            else:
                return f'{workers} / {requested}'
        
    def _widget_status(self):
        ### reporting proper number of nodes vs workers in a multi-GPU worker scenario
        workers = len(self.scheduler_info["workers"])
        
        if self.use_gpu:
            workers = int(workers / self.n_gpus_per_node)
            
        if hasattr(self, "worker_spec"):
            requested = sum(
                1 if "group" not in each else len(each["group"])
                for each in self.worker_spec.values()
            )
            
        elif hasattr(self, "workers"):
            requested = len(self.workers)
        else:
            requested = workers
            
        workers = self._format_workers(workers, requested, self.use_gpu, self.n_gpus_per_node)
            
        cores = sum(v["nthreads"] for v in self.scheduler_info["workers"].values())        
        cores_or_gpus = 'GPUs' if self.use_gpu else 'Cores'
        
        memory = sum(v["memory_limit"] for v in self.scheduler_info["workers"].values())
        memory = format_bytes(memory)
        
        
        text = """
<div>
  <style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }
    .dataframe tbody tr th {
        vertical-align: top;
    }
    .dataframe thead th {
        text-align: right;
    }
  </style>
  <table style="text-align: right;">
    <tr> <th>Workers</th> <td>%s</td></tr>
    <tr> <th>%s</th> <td>%s</td></tr>
    <tr> <th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            self._format_workers(workers, requested, self.use_gpu, self.n_gpus_per_node),
            cores_or_gpus,
            cores,
            memory,
        )
        return text
        
    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        if self.dashboard_link:
            dashboard_link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.dashboard_link,
                self.dashboard_link,
            )
        else:
            dashboard_link = ""
            
        if self.jupyter_link:
            jupyter_link = '<p><b>Jupyter: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.jupyter_link,
                self.jupyter_link,
            )
        else:
            jupyter_link = ""

        title = "<h2>%s</h2>" % self._cluster_class_name
        title = HTML(title)
        dashboard = HTML(dashboard_link)
        jupyter   = HTML(jupyter_link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        if self._supports_scaling:
            request = IntText(self.initial_node_count, description="Workers", layout=layout)
            scale = Button(description="Scale", layout=layout)

            minimum = IntText(1, description="Minimum", layout=layout)
            maximum = IntText(0, description="Maximum", layout=layout)
            adapt = Button(description="Adapt", layout=layout)

            accordion = Accordion(
                [HBox([request, scale]), HBox([minimum, maximum, adapt])],
                layout=Layout(min_width="500px"),
            )
            accordion.selected_index = None
            accordion.set_title(0, "Manual Scaling")
            accordion.set_title(1, "Adaptive Scaling")

            def adapt_cb(b):
                self.adapt(minimum=minimum.value, maximum=maximum.value)
                update()

            adapt.on_click(adapt_cb)

            def scale_cb(b):
                with log_errors():
                    n = request.value
                    with ignoring(AttributeError):
                        self._adaptive.stop()
                    self.scale(n)
                    update()

            scale.on_click(scale_cb)
        else:
            accordion = HTML("")

        box = VBox([title, HBox([status, accordion]), jupyter, dashboard])

        self._cached_widget = box

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=self.loop)
        self.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box
        
    def print_links_ComputeVM(self):
        self.__print_message(f"DASHBOARD: {self.scheduler_info['dashboard_url']}")

        self.__print_message(f"NOTEBOOK: {self.scheduler_info['jupyter_url']}")

    def scale(self, workers=1):
        if workers<=0:
            self.close()
            return
        
        count=len(self.workers_list)+1 # one more worker in head node
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
                    child_run.complete() # complete() will mark the run "Complete", but won't kill the process
                    child_run.cancel()
                else:
                    print("All scaled workers are removed.")
                    
    # close cluster
    async def _close(self):
        if(self.status=="closed"):
            return
        while self.workers_list:
            child_run=self.workers_list.pop()
            child_run.cancel()
            child_run.complete()
        if self.run:
            self.run.cancel()
            self.run.complete()
        await super()._close()
        self.status="closed"
        self.__print_message("Scheduler and workers are disconnected.")

    def close(self):
        asyncio.ensure_future(self._close())
