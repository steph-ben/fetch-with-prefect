# Fetching with Prefect

Current target is to download [NOAA GFS from AWS S3](https://registry.opendata.aws/noaa-gfs-bdp-pds/), using [Prefect machinery](https://prefect.io).


## Getting started

* Python reqs

```shell
virtualenv ve -p python3
pip install -r requirements.txt
```


* Test our flow locally, wihtout any prefect server
  
```shell
python fetcher.py run

[2020-12-16 22:04:09+0000] INFO - prefect.FlowRunner | Beginning Flow run for 'aws-gfs-download'
[2020-12-16 22:04:10+0000] INFO - prefect.TaskRunner | Task 'check_run_availability': Starting task run...
[2020-12-16 22:04:10+0000] INFO - prefect.TaskRunner | 20201215 / 0 : Checking run availability ...
[2020-12-16 22:04:26+0000] INFO - prefect.TaskRunner | 20201215 / 0 : Run is available !
...
[2020-12-16 22:05:30+0000] INFO - prefect.TaskRunner | Task 'timestep_9_download': Finished task run for task with final state: 'Success'
[2020-12-16 22:05:30+0000] INFO - prefect.TaskRunner | Task 'timestep_18_download': Finished task run for task with final state: 'Success'
[2020-12-16 22:05:30+0000] INFO - prefect.FlowRunner | Flow run SUCCESS: all reference tasks succeeded
```

Working great ? Now let's run it inside prefect server


* Launching prefect and one local agent

```shell
prefect server start
prefect agent local start
```

* Register and trigger our flow

```shell
# Needed once
$ prefect create project "gfs-fetcher"

# Install our app in virtualenv, so the local agent can access our lib
$ python setup.py develop

# Interracting with Prefect !
$ python fetcher.py register
Result check: OK
Flow URL: http://localhost:8080/default/flow/ec5a906b-6b03-46ce-b7d8-b02e73d30d98
 └── ID: 7f049f56-2c6d-49b4-8515-658b43850764
 └── Project: gfs-fetcher
 └── Labels: ['steph-laptop']
7f049f56-2c6d-49b4-8515-658b43850764

$ python fetcher.py trigger
{'data': {'flow': [{'id': '7f049f56-2c6d-49b4-8515-658b43850764', 'name': 'aws-gfs-download'}]}}
A new FlowRun has been triggered
Go check it on http://linux:8080/flow-run/1829c2f9-a5f4-43c4-ad3e-bd42a80068ea
```

Enjoy the view !