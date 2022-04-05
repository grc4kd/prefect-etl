import prefect
from prefect import task, Flow

@task
def load_to_sftp(all_data, sftp_targethostname: str, sftp_username: str, sftp_password: str):
    logger = prefect.context.get("logger")
    logger.info(f"Loading data to SFTP server at {sftp_targethostname}...")
    
    if (all_data != None):
        

with Flow("etl-flow") as flow:
    data_ext = extract()
    data_trn = transform(data_ext)
    load(data_trn)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
#flow.run()
