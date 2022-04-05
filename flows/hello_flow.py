import prefect
from prefect import task, Flow
from prefect.storage import GitHub

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")

with Flow("hello-flow") as flow:
    hello_task()

# storage can point to the same module
flow.storage = GitHub(repo="grc4kd/prefect-etl",
                      path="/flows/hello_flow.py",
                      access_token_secret="github_secret_grc4kd")
