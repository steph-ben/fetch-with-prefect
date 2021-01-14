"""
GFS flow for test running on my laptop
"""
import os
import sys

from fetchers.s3.flows import create_flow_download
from fetchers.utils import trigger_prefect_flow


prefect_project_name = "laptop-gfs-project"

settings = {
    'flow_name': "laptop-gfs-download",
    'timesteps': [0, 1, 2, 3, 4, 5, 6],
    'max_concurrent_download': 5,
    'download_dir': "/tmp/laptop/s3/gfs"
}


def main(cmd):
    # Create a prefect's flow object with some configuration
    flow_nwp_00 = create_flow_download(run=00, **settings)
    settings['max_concurrent_download'] = 5
    flow_nwp_12 = create_flow_download(run=12, **settings)

    if cmd in ("register", "trigger"):
        # Ensure the flow is well registered in prefect server
        for flow in flow_nwp_00, flow_nwp_12:
            r = flow.register(project_name=prefect_project_name)
            print(r)

        for flow in flow_nwp_00, flow_nwp_12:
            # For demo purpose, schedule a download each hour
            from prefect.schedules import Schedule
            from prefect.schedules.clocks import CronClock
            schedule = Schedule(clocks=[CronClock(f"0 * * * *")])
            flow.schedule = schedule
            r = flow.register(project_name=prefect_project_name)
            print(r)

    if cmd == "trigger":
        # Trigger the flow manually
        for flow in flow_nwp_00, flow_nwp_12:
            trigger_prefect_flow(
                flow_name=flow.name,
                run_name=f"{flow.name}-manually_triggered",
            )

    if cmd == "run":
        # Run a download from current process
        flow_nwp_00.schedule = None
        flow_nwp_00.run()


if __name__ == "__main__":
    cmd = "register"
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

    main(cmd)
