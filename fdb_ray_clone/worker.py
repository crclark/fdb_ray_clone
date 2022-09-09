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
import fdb_ray_clone.client as client

import argparse
import logging
import time
import random
from multiprocessing import Process
from dataclasses import dataclass
import pickle
import sys
import signal
import os
import traceback
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


class MaxRetriesExceededException(Exception):
    pass


@fdb.transactional
def _relinquish_old_claim(tr: fdb.Transaction, config: WorkerConfig) -> None:
    """Clean up any claim we may have already, in case we died while processing a future."""
    claimed_future_id = future.get_worker_claim_future(tr, config.ss, config.worker_id)
    if claimed_future_id is not None:
        logging.info(
            f"found pre-existing claimed future for this worker: {claimed_future_id}. Relinquishing claim."
        )
        f = future.get_future_state(tr, config.ss, claimed_future_id)
        future.relinquish_claim(tr, config.ss, f)


def _report_failure(
    config: WorkerConfig,
    f: future.Future[T],
    exception_message: str,
) -> Union[future.FailedFuture[T], future.ClaimedFuture[T]]:
    logging.exception(
        f"Failed to process future {f} because of exception {exception_message}"
    )
    try:
        fail_result: Union[
            future.FailedFuture[T], future.ClaimedFuture[T]
        ] = future.fail_future(config.db, config.ss, f, exception_message)
        return fail_result
    except Exception as e:
        logging.exception(
            f"Failed to report failure of future {f} because of exception {e}"
        )
        raise e


def _work_on_future_with_retries(config: WorkerConfig, f: future.Future[T]) -> T:
    while True:
        try:
            result = f.code(*f.dependencies)
            if not config.store.can_store(result):
                raise Exception(f"Result {result} is not picklable.")
        except Exception as e:
            exception_type, error, tb = sys.exc_info()
            error_lines = traceback.format_exception(exception_type, error, tb)
            exception_msg = "\n".join(error_lines)
            f = _report_failure(config, f, exception_msg)
            if isinstance(f, future.FailedFuture):
                raise MaxRetriesExceededException
            continue
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


# https://stackoverflow.com/a/20186516
def _pid_exists(pid: int) -> bool:
    if pid < 0:
        return False  # NOTE: pid == 0 returns True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:  # errno.ESRCH
        return False  # No such process
    except PermissionError:  # errno.EPERM
        return True  # Operation not permitted (i.e., process exists)
    else:
        return True  # no error, we can send a signal to the process


def heartbeat_loop(
    worker_id: future.WorkerId,
    worker_pid: int,
    subspace: fdb.Subspace,
    cluster_name: str,
    max_cpu: int,
    max_ram: int,
    max_gpu: int,
    started_at: int,
) -> None:
    db = fdb.open()
    store_client = StoreClient(worker_id.address, worker_id.port)
    while True:
        time.sleep(5)
        if _pid_exists(worker_pid):
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
            logging.error("Worker died. Heartbeat monitor exiting.")
            break


def worker_loop(
    worker_id: future.WorkerId,
    subspace: fdb.Subspace,
    cluster_name: str,
    max_cpu: int,
    max_ram: int,
    max_gpu: int,
    db: fdb.Database,
    store: StoreServer,
) -> None:
    client.init(cluster_name)
    config = WorkerConfig(
        worker_id=worker_id,
        db=db,
        ss=subspace,
        store=store,
        max_cpu=max_cpu,
        max_ram=max_ram,
        max_gpu=max_gpu,
    )
    _relinquish_old_claim(db, config)
    while True:
        try:
            _process_one_future(config)
        except MaxRetriesExceededException:
            continue
        except Exception as e:
            logging.exception(
                f"Caught unexpected exception {e} while processing future. Abandoning this future and trying another. This is probably a bug in fdb_ray_clone."
            )


def main(
    worker_id: future.WorkerId,
    subspace: fdb.Subspace,
    cluster_name: str,
    max_cpu: int,
    max_ram: int,
    max_gpu: int,
) -> None:
    logging.basicConfig(level=logging.INFO)

    db = fdb.open()
    with StoreServer(bind_address=worker_id.address, bind_port=worker_id.port) as store:
        heartbeat_worker = Process(
            target=heartbeat_loop,
            args=(
                worker_id,
                os.getpid(),
                subspace,
                cluster_name,
                max_cpu,
                max_ram,
                max_gpu,
                future.seconds_since_epoch(),
            ),
        )
        heartbeat_worker.start()
        try:
            worker_loop(
                worker_id, subspace, cluster_name, max_cpu, max_ram, max_gpu, db, store
            )
        except KeyboardInterrupt:
            logging.info("Caught KeyboardInterrupt. Exiting.")
            heartbeat_worker.terminate()
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", help="address on which to serve the store", default="localhost"
    )
    parser.add_argument(
        "--port", type=int, help="port on which to serve the store", default=50001
    )
    parser.add_argument(
        "cluster_name",
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
        args.cluster_name,
        args.max_cpu,
        args.max_ram,
        args.max_gpu,
    )
