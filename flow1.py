import prefect
from prefect import task, Flow
from prefect.executors.dask import DaskExecutor


def info(msg: str):
    logger = prefect.context.get("logger")
    logger.info(msg)


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
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)


flow.executor = DaskExecutor(
    "a7354e8640c6d48b79968a8930326aa8-981355592.ap-southeast-2.elb.amazonaws.com:8786"
)
flow.run()
