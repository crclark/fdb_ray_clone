from typing import Any, Dict, List, Set, Tuple, Generator
import pytest
import uuid
from uuid import UUID

import fdb_ray_clone.control.future as future
from fdb_ray_clone.data.store import StoreServer, StoreClient
import fdb_ray_clone.worker as worker
import fdb_ray_clone.client as client

import fdb

fdb.api_version(710)


@pytest.fixture
def cluster_name() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def db() -> fdb.Database:
    db = fdb.open()
    return db


@pytest.fixture
def subspace(db: fdb.Database, cluster_name: str) -> fdb.Subspace:
    ss = fdb.Subspace(prefixTuple=("fdb_ray_clone", cluster_name))
    yield ss
    db.clear_range(ss.range().start, ss.range().stop)


@pytest.fixture
def test_client(cluster_name: str) -> client.Client:
    return client.Client(cluster_name)


def test_worker_restarted_objects_lost(
    db: fdb.Database,
    subspace: fdb.Subspace,
    test_client: client.Client,
) -> None:
    f = test_client.submit_future(
        lambda x, y: x + y,  # type: ignore [no-any-return]
        1,
        2,
    )
    assert test_client.ss.key() == subspace.key()
    address = "localhost"
    port = 50001
    with StoreServer(address, port) as store:

        worker_config = worker.WorkerConfig(
            worker_id=future.WorkerId(address, port),
            db=db,
            ss=subspace,
            store=store,
            max_cpu=1,
            max_ram=1**10 ^ 9,
            max_gpu=0,
        )
        worker._process_one_future(worker_config)
        future_result = test_client.await_future(f, time_limit_secs=0)
        assert future_result == 3

    with StoreServer(address, port) as store:
        with pytest.raises(client.WorkerDiedException):
            test_client.await_future(
                f,
                time_limit_secs=0,
                presume_worker_dead_after_secs=5,
                allow_resubmit=False,
            )
