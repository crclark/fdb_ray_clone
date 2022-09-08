"""
How do workers work?

We need the following processes per worker:
- A worker process that is responsible for finding and realizing futures.
- A store process that is responsible for storing and serving objects.
- A heartbeat process that monitors the other two processes and sends heartbeats
  to FoundationDB.

Something that is unclear to me is how the worker stores stuff in the store if
the store is a separate process. I guess we need a queue or something. For the
first version, let's try keeping the worker and store in the same process and
see what happens. Maybe the SharedMemoryManager server is already running on a
separate process and we won't have a problem. TODO: test this by storing something
and then have the worker busy loop and see if the store still responds to requests.
"""

import fdb_ray_clone.control.future as future
from fdb_ray_clone.data.store import StoreServer, StoreClient

import argparse
import logging
import time
import random
from multiprocessing import Process
from dataclasses import dataclass
import pickle
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Set,
    TypeVar,
    Union,
)

import fdb

fdb.api_version(710)

T = TypeVar("T")


@dataclass(frozen=True)
class WorkerConfig:
    worker_id: future.WorkerId
    db: fdb.Database
    ss: fdb.Subspace
    store: StoreServer
    max_cpu: int
    max_ram: int
    max_gpu: int


def _report_failure(
    config: WorkerConfig, f: future.Future[T], e: Exception
) -> Union[future.FailedFuture[T], future.ClaimedFuture[T]]:
    logging.exception(f"Failed to process future {f} because of exception {e}")
    try:
        fail_result: Union[
            future.FailedFuture[T], future.ClaimedFuture[T]
        ] = future.fail_future(config.db, config.ss, f, e)
        return fail_result
    except Exception as e:
        logging.exception(
            f"Failed to report failure of future {f} because of exception {e}"
        )
        # TODO: this will kill the worker. Add more catches to prevent that.
        raise e


def _work_on_future_with_retries(config: WorkerConfig, f: future.Future[T]) -> T:
    while True:
        try:
            result = f.code(*f.dependencies)
            if not config.store.can_store(result):
                raise Exception(f"Result {result} is not picklable.")
        except Exception as e:
            f = _report_failure(config, f, e)
            if isinstance(f, future.FailedFuture):
                raise e
        return result


def _current_resources(config: WorkerConfig) -> future.WorkerResources:
    return future.WorkerResources(
        cpu=config.max_cpu,
        ram=config.max_ram - config.store.used_ram,
        gpu=config.max_gpu,
    )


@fdb.transactional
def _claim_one_future(
    tr: fdb.Transaction, config: WorkerConfig
) -> Optional[future.ClaimedFuture[T]]:
    current_resources = _current_resources(config)
    eligible_futures = future.futures_fitting_resources(
        tr, config.ss, current_resources
    )
    if eligible_futures:
        future_id = random.choice(eligible_futures)
        f: future.ClaimedFuture[T] = future.claim_future(
            tr, config.ss, future_id, config.worker_id.address, config.worker_id.port
        )
        return f
    else:
        return None


def _store_result(config: WorkerConfig, f: future.Future[T], result: T) -> str:
    buffer = config.store.store_local(result)
    return buffer.name


def _realize_future(
    config: WorkerConfig, f: future.Future[T], buffer_name: str
) -> None:
    logging.info("Realizing future %s with buffer %s", f, buffer_name)
    future.realize_future(config.db, config.ss, f, buffer_name)


def _process_one_future(config: WorkerConfig) -> None:
    f = _claim_one_future(config.db, config)
    if f is None:
        # TODO: make configurable
        time.sleep(5)
        return None
    else:
        logging.info("Found a future to process: %s", f)
        result = _work_on_future_with_retries(config, f)
        buffer_name = _store_result(config, f, result)
        _realize_future(config, f, buffer_name)


def worker_loop(
    worker_id: future.WorkerId,
    subspace: fdb.Subspace,
    max_cpu: int,
    max_ram: int,
    max_gpu: int,
) -> None:
    db = fdb.open()
    with StoreServer(bind_address=worker_id.address, bind_port=worker_id.port) as store:
        config = WorkerConfig(
            worker_id=worker_id,
            db=db,
            ss=subspace,
            store=store,
            max_cpu=max_cpu,
            max_ram=max_ram,
            max_gpu=max_gpu,
        )
        while True:
            try:
                _process_one_future(config)
            except Exception as e:
                logging.exception(
                    f"Caught unexpected exception {e} while processing future. Abandoning this future and trying another. This is probably a bug in fdb_ray_clone."
                )


def main(
    worker_id: future.WorkerId,
    subspace: fdb.Subspace,
    max_cpu: int,
    max_ram: int,
    max_gpu: int,
) -> None:
    logging.basicConfig(level=logging.INFO)
    worker = Process(
        target=worker_loop,
        args=(
            worker_id,
            subspace,
            max_cpu,
            max_ram,
            max_gpu,
        ),
    )

    started_at = future.seconds_since_epoch()
    worker.start()

    # TODO: find a better way to avoid a race with the startup of the
    # SharedMemoryManager. If we are too fast, StoreClient fails to start up
    # below (connection refused). Can probably use a multiprocessing queue to
    # wait for a ready signal from the worker.
    time.sleep(5)
    db = fdb.open()
    store_client = StoreClient(worker_id.address, worker_id.port)
    while True:
        time.sleep(5)
        if worker.is_alive():
            try:
                current_resources = future.WorkerResources(
                    cpu=max_cpu,
                    ram=max_ram - store_client.get_used_ram(),
                    gpu=max_gpu,
                )
                heartbeat = future.WorkerHeartbeat(
                    last_heartbeat_at=future.seconds_since_epoch(),
                    started_at=started_at,
                    available_resources=current_resources,
                )
                future.write_worker_heartbeat(db, subspace, worker_id, heartbeat)
            except Exception as e:
                logging.exception(f"Failed to write heartbeat because of exception {e}")
        else:
            logging.error("Worker died. Exiting.")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", help="address on which to serve the store", default="localhost"
    )
    parser.add_argument(
        "--port", type=int, help="port on which to serve the store", default=50001
    )
    parser.add_argument(
        "--cluster_name",
        help="namespace in which to store futures. Must be the same on all workers and clients.",
    )
    parser.add_argument(
        "--max_cpu",
        type=int,
        help="maximum number of CPUs that this worker can use. Default is 1.",
        default=1,
    )
    parser.add_argument(
        "--max_ram",
        type=int,
        help="maximum amount of RAM (in bytes) that this worker can use. Default is 2GiB",
        default=2 * 1024 * 1024 * 1024,
    )
    parser.add_argument(
        "--max_gpu",
        type=int,
        help="maximum number of GPUs that this worker can use. Default is 0.",
        default=0,
    )
    args = parser.parse_args()
    main(
        future.WorkerId(args.address, args.port),
        fdb.Subspace(("fdb_ray_clone", args.cluster_name)),
        args.max_cpu,
        args.max_ram,
        args.max_gpu,
    )
