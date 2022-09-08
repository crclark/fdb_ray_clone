from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple
import pytest
import uuid
from uuid import UUID


import fdb_ray_clone.control.future as future
from fdb_ray_clone.data.store import StoreServer, StoreClient
import fdb_ray_clone.worker as worker

import fdb

fdb.api_version(710)


@pytest.fixture
def db() -> fdb.Database:
    db = fdb.open()
    return db


@pytest.fixture
def subspace(db: fdb.Database) -> fdb.Subspace:
    ss = fdb.Subspace(prefixTuple=(b"test_fdb_ray_clone", uuid.uuid4()))
    yield ss
    db.clear_range(ss.range().start, ss.range().stop)


@pytest.fixture
def worker_config(db: fdb.Database, subspace: fdb.Subspace) -> worker.WorkerConfig:  # type: ignore
    address = "localhost"
    port = 50001
    with StoreServer(address, port) as store:

        yield worker.WorkerConfig(
            worker_id=future.WorkerId(address, port),
            db=db,
            ss=subspace,
            store=store,
            max_cpu=1,
            max_ram=1**10 ^ 9,
            max_gpu=0,
        )


def test_worker_process_one_future(worker_config: worker.WorkerConfig) -> None:
    f = future.submit_future(
        worker_config.db,
        worker_config.ss,
        lambda x, y: x + y,
        [1, 2],
        future.ResourceRequirements(),
    )
    store_client = StoreClient(
        worker_config.worker_id.address, worker_config.worker_id.port
    )
    worker._process_one_future(worker_config)
    future_result = future.await_future(
        worker_config.db, worker_config.ss, f, time_limit_secs=0
    )
    buffer_name = future_result.name
    result = store_client.get(buffer_name)
    assert result == 3
