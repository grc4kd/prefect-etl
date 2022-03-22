from prefect import Flow
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

# Configure extra environment variables for this flow,
# and set a custom image
with Flow("test_docker_agent") as flow:
    flow.run_config = DockerRun(
        env={"SOME_VAR": "VALUE"},
        image="prefecthq/prefect:latest"
    )

# run a different python file as a flow from GitHub storage
flow.storage = GitHub(repo="grck4d/prefect-etl", path="/flows/tutorial_03.py")
