import prefect
from prefect import task, Flow


@task
def report_start_day():
    logger = prefect.context.get("logger")
    logger.info(prefect.context.today)

with Flow('My flow') as flow:
    print(prefect.context.today)

    report_start_day()


flow.run()
