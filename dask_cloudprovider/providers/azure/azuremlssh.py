from azureml.core import Experiment, RunConfiguration, ScriptRunConfig
from azureml.core.compute import AmlCompute
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration

from dask.distributed import SSHCluster

class AzureMLSSHCluster:
    def __init__(
        self,
        workspace,
        compute_target,
        environment_definition,
        experiment_name='dask-cloudprovider',
        initial_worker_count=2,
        threads_per_worker=2,
        asynchronous=False,
        **kwargs,
    ):
        self.workspace = workspace
        self.compute_target = compute_target
        self.environment_definition = environment_definition
        self.experiment_name = experiment_name
        self.initial_worker_count = initial_worker_count
        self.threads_per_worker = threads_per_worker
        self.asynchronous = asynchronous

        ## put supported vm size in constants file
        self.supported_vm_size = {
            'STANDARD_D14_V2' : 16
        }

        self.live_ip_address = []
        self.worker_per_node = 1
        ## start cluster
        self.calc_node_count()

        self.cluster = self.create_cluster()

    def calc_node_count(self):
        serialized_json = compute_target.serialize()
        ## handle exception
        vm_size = status = serialized['properties']['status']['vmSize']

        core_count_per_node = 0
        if vm_size in self.supported_vm_size:
            core_count_per_node = self.supported_vm_size[vm_size]
        else:
            raise ValueError("The compute target %s has unsupported vm size" %(self.compute_target))

        if self.threads_per_worker > core_count_per_node:
            raise ValueError("The requirement of threads per worker cannot be met")

        self.worker_per_node = core_count_per_node / self.threads_per_worker
        ## add one more to initial worker count as scheduler
        node_count_required = (self.initial_worker_count + 1) / self.worker_per_node

        return node_count_required

    
    def create_cluster(self):
        exp = Experiment(self.workspace, self.experiment_name)
        estimator = Estimator(
            os.path.join(self.abs_path, "setup"),
            compute_target=self.compute_target,
            entry_script="start.py",  ## start an ip
            environment_definition=self.environment_definition,
            script_params=self.scheduler_params,
            node_count=1,  ### start only scheduler
            distributed_training=MpiConfiguration(),
            use_docker=True,
            inputs=self.datastores,
        )

        run = exp.submit(estimator, tags=self.tags)

        print("Waiting for scheduler node's IP")

        while (
            run.get_status() != "Canceled"
            and run.get_status() != "Failed"
            and "scheduler" not in run.get_metrics()
        ):
            print(".", end="")
            logger.info("Scheduler not ready")
            time.sleep(5)

        if run.get_status() == "Canceled" or run.get_status() == "Failed":
            logger.exception("Failed to start the AzureML cluster")
            raise Exception("Failed to start the AzureML cluster.")

        print("\n\n")

        ip_address = run.get_metrics()["ip"]
        cluster = SSHCluster(
            [ip, ip, ip, ip],
            connect_options={"known_hosts": None},
            worker_options={"nthreads": self.threads_per_worker},
            scheduler_options={"port": 0, "dashboard_address": ":8797"}
        )
        return cluster


