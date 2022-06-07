
import prefect
from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.executors.dask import DaskExecutor


def info(msg: str):
    logger = prefect.context.get("logger")
    logger.info(msg)


@task
def generate_range(provided_range):
    return range(int(provided_range))


@task
def inc(x):
    return x + 1


@task
def dec(x):
    return x - 1


@task
def add(x, y):
    for _ in range(x):
        for _ in range(y):
            continue
    info(f"x + y = {x + y}")
    return x + y


@task
def list_sum(arr):
    info(f"sum is {sum(arr)}")
    return sum(arr)


with Flow("dask-example") as flow:
    provided_range = Parameter(name="range", required=True)
    generated_range = generate_range(provided_range)
    incs = inc.map(x=generated_range)
    decs = dec.map(x=generated_range)
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)


flow.storage = GitHub(repo="sayujnath/prefect-dask-flow", path="flow1.py")
flow.executor = DaskExecutor(
    "dask-ui.abyssfabric.co:8786"
)
# flow.run()

flow.register(project_name="secondproject")

# in ~/.prefect/config.toml file, need to add
# [server]
#     host = prefect.canditude.com
#     port = 8080
