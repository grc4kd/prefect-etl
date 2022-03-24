import prefect
from prefect import Flow, task, flatten
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
from prefect.executors import DaskExecutor


@task
def extract():
    logger = prefect.context.get("logger")
    logger.info("Extracting...")
    data = [123, "234", "ball", 0, 345, 456, 567, 234, 456, 123]
    return data


@task
def transform(data):
    logger = prefect.context.get("logger")
    logger.info(f"Transforming {len(data)} array elements.")

    # a sub-routine to do some quick math
    def subt(elem):
        x = None

        try:
            # convert input to integer as needed
            x = int(elem)
        except Exception:
            # if no conversion is possible, default to 0
            x = 0

        # all inputs besides zero work
        if x != 0:
            return 2 / x
        # 2 / x would divide by zero, return -1 instead
        if x == 0:
            return int.from_bytes(b'\xff\xff', byteorder='big', signed=True)

    return [subt(x) for x in data]


@task
def load(data):
    logger = prefect.context.get("logger")
    logger.info("Loading data...")
    # normally we would want to keep the data
    # OUT of the prefect cloud logs
    logger.info(str(data))
    # write / load data to new text file
    with open("new_data.txt", "w") as f:
        f.write(str(data) + "\n")
        f.close()


# Configure extra environment variables for this flow,
# and set a custom image
with Flow("test_docker_agent") as flow:
    flow.run_config = DockerRun(
        image="prefecthq/prefect:latest"
    )
    # let's run a bunch of functions x10 range mapping
    data_list = [extract() for i in range(10)]

    # join extract outputs into one big list by using flat-mapping
    data_trn = transform(data=flatten(data_list))

    # load the data using a mapping function
    load(data=data_trn)

# storage can point to the same module
flow.storage = GitHub(repo="grc4kd/prefect-etl",
                      path="/flows/tutorial_docker_agent.py",
                      access_token_secret="github_secret_grc4kd")

# By default this will use a temporary local Dask cluster
flow.executor = DaskExecutor(
    cluster_kwargs={"n_workers": 2, "threads_per_worker": 2}
)
