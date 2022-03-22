import prefect
from prefect import Flow, task
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

@task
def extract():
    logger = prefect.context.get("logger")
    logger.info("Extracting...")
    all_data = [123, "234", "ball", 0, 345, 456, 567, 234, 456, 123]
    return all_data

@task
def transform(all_data):
    logger = prefect.context.get("logger")
    logger.info(f"Transforming {len(all_data)} array elements.")

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
    
    return [subt(x) for x in all_data]

@task
def load(all_data):
    logger = prefect.context.get("logger")
    logger.info("Loading data...")
    # normally we would want to keep the data
    # OUT of the prefect cloud logs
    logger.info(str(all_data))
    # write / load data to new text file
    with open("new_data.txt", "w") as f:
        f.write(str(all_data) + "\n")
        f.close()

# Configure extra environment variables for this flow,
# and set a custom image
with Flow("test_docker_agent") as flow:
    flow.run_config = DockerRun(
        image="prefecthq/prefect:latest"
    )
    # let's run a bunch of functions x10
    data_ext = [extract() for i in range(10)]
    data_trn = [transform(de) for de in data_ext]
    for dt in data_trn:
        load(dt)

# storage can point to the same module
flow.storage = GitHub(repo="grc4kd/prefect-etl",
                      path="/flows/tutorial_docker_agent.py",
                      access_token_secret="github_secret_grc4kd")
