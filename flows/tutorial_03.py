import prefect
from prefect import task, Flow

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

    def subt(elem):
        x = 0
        try:
            x = int(elem)
        except Exception:
            x = 0
        if x != 0:
            return 2 / x
        if x == 0:
            return int.from_bytes(b'\xff\xff', byteorder='big', signed=True)
    # divide by two
    # if input is zero return zero (don't divide by zero)
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

with Flow("etl-flow") as flow:
    data_ext = extract()
    data_trn = transform(data_ext)
    load(data_trn)

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")
#flow.run()
