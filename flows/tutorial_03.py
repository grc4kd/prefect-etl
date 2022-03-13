import prefect
from prefect import task, Flow

@task
def extract():
    logger = prefect.context.get("logger")
    logger.info("Extracting...")
    all_data = [123, 234, 345, 456, 567, 234, 456, 123]
    return all_data

@task
def transform(all_data):
    logger = prefect.context.get("logger")
    logger.info(f"Transforming {len(all_data)} array elements.")
    # divide by two
    # if input is zero return zero (don't divide by zero)
    all_data = [0 if 0 else x / 2 for x in all_data]

@task
def load(all_data):
    logger = prefect.context.get("logger")
    logger.info("Loading...")
    # write / load data to new text file
    with open("new_data.txt", "w") as f:
        f.write(str(all_data) + "\n")
        f.close()

with Flow("etl-flow") as flow:
    data = extract()
    transform(data)
    load(data)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
