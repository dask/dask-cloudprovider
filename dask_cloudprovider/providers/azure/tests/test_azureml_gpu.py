class aml_test:
    def __init__(self):
        from dask_cloudprovider import AzureMLCluster  # noqa


if __name__ == "__main__":
    amlcl = aml_test()
