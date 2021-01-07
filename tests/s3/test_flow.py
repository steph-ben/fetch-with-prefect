from fetchers.s3.flows import flow_download


def test_flow():
    flow_run = flow_download.run()
    print(type(flow_run))
