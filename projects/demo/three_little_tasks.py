"""
A quick demo of three little shell tasks
"""
import sys
from pathlib import Path

import prefect
from prefect import Flow, task
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.shell import ShellTask


shelltask = ShellTask()


@task(log_stdout=True)
def show_file():
    with Path("/tmp/cron.me") as fd:
        print(fd.read_text())


with Flow("three_little_tasks_flow") as flow:
    t1 = shelltask(command="echo '====== start' >> /tmp/cron.me")
    t2 = shelltask(command="date >> /tmp/cron.me; sleep 3")
    t3 = shelltask(command="echo '====== stop' >> /tmp/cron.me")

    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(show_file)


if __name__ == "__main__":
    cmd = "run"
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

    if cmd == "run":
        flow.run()

    if cmd == "schedule":
        flow.schedule = Schedule(clocks=[CronClock("* * * * *")])
        flow.run()

    if cmd == "register":
        flow.schedule = Schedule(clocks=[CronClock("* * * * *")])
        r = flow.register(project_name="demo")
