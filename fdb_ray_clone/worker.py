"""
How do workers work?

We need the following processes per worker:
- A worker process that is responsible for finding and realizing futures.
- A store process that is responsible for storing and serving objects.
- A heartbeat process that monitors the other two processes and sends heartbeats
  to FoundationDB.
"""

import fdb_ray_clone.control.future as future
from fdb_ray_clone.data.store import StoreServer, StoreClient
import fdb_ray_clone.client as client

from uuid import UUID
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
import random
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


def _exception_to_string(e: Exception) -> str:
    exception_type, error, tb = sys.exc_info()
    error_lines = traceback.format_exception(exception_type, error, tb)
    exception_msg = "\n".join(error_lines)
    return exception_msg


def _work_on_future_with_retries(config: WorkerConfig, f: future.Future[T]) -> T:
    while True:
        try:
            result = f.code(*f.args)
            if not config.store.can_store(result):
                raise Exception(f"Result {result} is not picklable.")
        except Exception as e:
            exception_msg = _exception_to_string(e)
            f = _report_failure(config, f, exception_msg)
            if isinstance(f, future.FailedFuture):
                raise MaxRetriesExceededException
            continue
        return result


# TODO: need comprehensive tests for this. Test all cases, and test that it
# works recursively with chains of dependencies.
@fdb.transactional
def _claim_assigned_future(
    tr: fdb.Transaction, config: WorkerConfig
) -> Optional[future.ClaimedFuture[T]]:
    """Claim a future that was assigned to this worker by the LocalityRequirement
    system. These are futures that the client believed that only this particular
    worker can process because this worker holds a particular object."""

    logging.info("Entered _claim_assigned_future")

    @fdb.transactional
    def claim_or_reassign(  # type: ignore [return] # TODO: hitting this mpypy bug (?) a lot
        tr: fdb.Transaction,
        assigned_future_id: UUID,
        why_assigned: future.ObjectRef[Any],
    ) -> Optional[future.ClaimedFuture[T]]:
        """For a future assigned to this worker by the LocalityRequirement system,
        check whether we still have the objectref needed by this future. If not,
        check whether another worker has a newer ObjectRef containing the same
        object and reassign to that worker. If not, resubmit the future that
        created the object and continue. See worker_assignments subspace
        design doc for more details."""
        assigned_future = future.get_future_state(tr, config.ss, assigned_future_id)
        logging.info(f"Examining assigned future {assigned_future}")
        if config.store.has_name(why_assigned.name):
            # We still have the objectref needed by this future. Claim it.
            future.clear_worker_assignment(
                tr, config.ss, config.worker_id, assigned_future_id
            )

            claimed_future: future.ClaimedFuture[T] = future.claim_future(
                tr,
                config.ss,
                assigned_future,
                config.worker_id.address,
                config.worker_id.port,
            )
            logging.info(
                f"Worker has object in store. Claimed assigned future {assigned_future}."
            )
            return claimed_future
        else:
            # We no longer have the objectref needed by this future. Check if
            # another worker has a newer ObjectRef containing the same object.
            # If so, reassign to that worker. If not, resubmit the future that
            # created the object and continue.

            upstream_future = future.get_future_state(
                tr, config.ss, why_assigned.future_id
            )

            logging.info(
                f"Worker has no object in store. Upstream future is {upstream_future}."
            )
            match upstream_future:
                case future.UnclaimedFuture() | future.ClaimedFuture():
                    # Someone will claim or has claimed this work.
                    # The upstream future will be eventually realized and a later
                    # iteration of this loop will reassign assigned_future
                    # to the claimant. We can't do it yet because we need to
                    # supply the why_assigned field with an ObjectRef, which
                    # we don't have yet.
                    logging.info(
                        f"Upstream future is already being recomputed. Looking for other assigned futures while we wait for upstream."
                    )
                    return None
                case future.FailedFuture(latest_exception=upstream_exception):
                    # Someone (maybe even us) tried to re-try the upstream
                    # future but it hit its max retries limit. Our assignedassigned_future_id
                    # future can't be realized because this dependency is missing,
                    # so we need to transition our assigned future to the failed state.

                    logging.info(
                        f"Upstream future has failed. Failing assigned future."
                    )
                    future.clear_worker_assignment(
                        tr, config.ss, config.worker_id, assigned_future_id
                    )
                    # State machine only allows us to fail a future we have claimed,
                    # so we need to claim it first.
                    claimed_future = future.claim_future(
                        tr,
                        config.ss,
                        assigned_future,
                        config.worker_id.address,
                        config.worker_id.port,
                    )
                    future.fail_future(
                        tr,
                        config.ss,
                        claimed_future,
                        _exception_to_string(
                            client.UpstreamDependencyFailedException(upstream_exception)
                        ),
                        fail_permanently=True,
                    )
                    return None
                case future.RealizedFuture(
                    latest_result=upstream_result
                ) if upstream_result == why_assigned:
                    # The upstream future was last computed by this worker, before it crashed. It
                    # needs to be resubmitted.
                    future.resubmit_realized_future_bad_object_ref(
                        tr, config.ss, upstream_future.id, why_assigned
                    )
                    return None
                case future.RealizedFuture(
                    latest_result=upstream_result
                ) if upstream_result != why_assigned:
                    # Someone (maybe even us) has already realized the upstream
                    # future. We can now reassign our assigned future to that
                    # worker.
                    # TODO: we could have a fast path here for the special case
                    # where the current worker re-realized the future.
                    logging.info(
                        "Upstream future has been realized. Reassigning this assigned future to the worker that now has the upstream object."
                    )
                    future.clear_worker_assignment(
                        tr, config.ss, config.worker_id, assigned_future_id
                    )
                    future.write_worker_assignment(
                        tr,
                        config.ss,
                        upstream_result.worker_id,
                        assigned_future_id,
                        upstream_result,
                    )
                    return None

    result: Optional[future.ClaimedFuture[T]] = future.iterate_worker_assignments(
        tr, config.ss, config.worker_id, claim_or_reassign
    )
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
    logging.info("Entered _claim_one_future")
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
    # To avoid deadlock, randomize whether we look at assigned futures or
    # any future that fits on this worker.
    logging.info("Looking for future to process.")
    f = (
        _claim_one_future(config.db, config)
        or _claim_assigned_future(config.db, config)
        if random.choice([True, False])
        else _claim_assigned_future(config.db, config)
        or _claim_one_future(config.db, config)
    )
    logging.info(f"Found {f}")

    if f is None:
        # TODO: make configurable
        logging.debug("No work to do. Sleeping for 5 seconds.")
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
    logging.info("Cleaning up any stale state.")
    _relinquish_old_claim(db, config)
    logging.info("Starting worker loop.")
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
