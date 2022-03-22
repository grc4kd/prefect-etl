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

# storage can point to the same module
flow.storage = GitHub(repo="grc4kd/prefect-etl",
                      path="/flows/tutorial_docker_agent.py",
                      access_token_secret="github_secret_grc4kd")
