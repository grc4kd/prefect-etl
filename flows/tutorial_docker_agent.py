import prefect
from prefect import Flow, task, flatten
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
from prefect.executors import DaskExecutor


@task
def extract():
    logger = prefect.context.get("logger")
    logger.info("Extracting...")
    data = [123, "234", "ball", 0, 345, 456, 567, 234, 456, 123,
            "e2e2", 123, -23, "fun guy", 3302]
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
            return 4 / x
        # y / 0 would divide by zero, return -1 instead
        if x == 0:
            return int.from_bytes(b'\xff\xff', byteorder='big', signed=True)

    return [subt(x) for x in data]


@task
def load(data):
    logger = prefect.context.get("logger")
    logger.info(f"Loading data, {len(data)} records.")
    
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
    # let's run a bunch of functions range mapping
    data_list = extract()

    # join extract outputs into one big list by using flat-mapping
    data_trn = [transform(data=data_list) for i in range(100)]

    # load the data using a mapping function
    load(data=data_trn)

# storage can point to the same module
flow.storage = GitHub(repo="grc4kd/prefect-etl",
                      path="/flows/tutorial_docker_agent.py",
                      access_token_secret="github_secret_grc4kd")

# By default this will use a temporary local Dask cluster
flow.executor = DaskExecutor()
