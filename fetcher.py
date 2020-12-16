import os
import datetime
import logging
import sys

import prefect
from prefect import Parameter
from prefect.engine import signals
from prefect.engine.executors import LocalDaskExecutor

from fetchers.s3 import NoaaGfsS3

logger = logging.getLogger(__name__)


settings = {
    'timesteps': [3, 6, 9, 12, 15, 18],
    'download_dir': '/tmp/plop'
}


########################################################################


@prefect.task(
    log_stdout=True,
    max_retries=5, retry_delay=datetime.timedelta(minutes=1)
)
def check_run_availability(date_day: Parameter, run: Parameter):
    """
    Check if a GFS run is available for a given day

    :param date_day:
    :param run:
    :return:
    """
    print(f"{date_day} / {run} : Checking run availability ...")
    s3api = NoaaGfsS3()
    r = s3api.filter(Prefix=s3api.get_daterun_prefix(str(date_day), str(run))).limit(count=1)
    if len(list(r)) > 0:
        print(f"{date_day} / {run} : Run is available !")
        return {'date_day': date_day, 'run': run}
    else:
        raise signals.FAIL(f"Run {date_day} / {run} is not yet available")


@prefect.task(
    log_stdout=True,
    max_retries=5, retry_delay=datetime.timedelta(minutes=1)
)
def check_timestep_availability(daterun_info: dict, timestep: str):
    """
    Check if a particular timestep for GFS is available

    :param daterun_info:
    :param timestep:
    :return:
    """
    print(f"{daterun_info} / {timestep} : Checking timestep availability ...")
    s3api = NoaaGfsS3()
    r = s3api.filter(Prefix=s3api.get_timestep_key(timestep=timestep, **daterun_info))
    if len(list(r)) > 0:
        daterun_info = dict(**daterun_info)
        daterun_info['timestep'] = timestep
        print(f"{daterun_info} : Timestep available !")
        return daterun_info
    else:
        raise ValueError(f"Timestep {timestep} not yet available")


@prefect.task(log_stdout=True)
def download_timestep(timestep_info: dict, download_dir: str):
    """
    Download a specific timestep file

    :param timestep_info:
    :param download_dir:
    :return:
    """
    print(f"Downloading file {timestep_info} to {download_dir} ...")
    s3api = NoaaGfsS3()
    s3api.download(
        object_key=s3api.get_timestep_key(**timestep_info),
        destination_dir=download_dir
    )


with prefect.Flow(name="aws-gfs-download") as flow:
    date_day = prefect.Parameter("date_day", default="20201215")
    run = prefect.Parameter("run", default=0)

    daterun_avail = check_run_availability(
        date_day=date_day, run=run
    )

    for timestep in settings['timesteps']:
        timestep_avail = check_timestep_availability(
            daterun_info=daterun_avail, timestep=timestep,
            task_args={'name': f'timestep_{timestep}_check_availability'}
        )
        download_timestep(
            timestep_info=timestep_avail,
            download_dir=settings['download_dir'],
            task_args={'name': f'timestep_{timestep}_download'}
        )

flow.executor = LocalDaskExecutor(
    scheduler="threads",
    num_workers=16
)

if __name__ == "__main__":
    cmd = "run"
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

    if cmd == "register":
        os.environ['PREFECT__SERVER__HOST'] = 'linux'
        registration = flow.register("gfs-fetcher")
        print(registration)
    elif cmd == "run":
        flow.run()
        #flow.visualize()
    elif cmd == "trigger":
        client = prefect.Client()
        query = client.graphql({
            'query':
                {'flow(where: {archived: {_eq: false}, name: {_eq: "aws-gfs-download"}})':
                     ['id', 'name']
                 }
        })
        print(query)
        flow_id = query['data']['flow'][0]['id']
        r = client.create_flow_run(flow_id=flow_id, parameters={
            'run': 12
        })
        print(f"A new FlowRun has been triggered")
        print(f"Go check it on http://linux:8080/flow-run/{r}")

