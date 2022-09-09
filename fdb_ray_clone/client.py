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
from dataclasses import dataclass
import logging


import fdb_ray_clone.control.future as future
import fdb_ray_clone.data.store as store

import fdb

fdb.api_version(710)

T = TypeVar("T")


@dataclass(frozen=True)
class Future(Generic[T]):
    _future: future.Future[T]


# TODO: how does the code within futures get access to a client? We might need
# a global or something awful like that.


class Client(object):
    def __init__(self, cluster_name: str):
        self.ss = fdb.Subspace(("fdb_ray_clone", cluster_name))
        self.db = fdb.open()
        self.clients: Dict[future.WorkerId, store.StoreClient] = dict()

    def submit_future(
        self,
        fn: Callable[..., T],
        *args: Any,
        min_cpu: int = 0,
        min_ram: int = 0,
        min_gpu: int = 0
    ) -> Future[T]:
        resources = future.ResourceRequirements(min_cpu, min_ram, min_gpu)
        return Future(
            _future=future.submit_future(self.db, self.ss, fn, args, resources)
        )

    def _get_or_create_client(self, worker_id: future.WorkerId) -> store.StoreClient:
        if worker_id not in self.clients:
            self.clients[worker_id] = store.StoreClient(
                worker_id.address, worker_id.port
            )
        return self.clients[worker_id]

    def await_future(
        self,
        f: Future[T],
        time_limit_secs: Optional[int] = None,
        presume_worker_dead_after_secs: int = 30,
    ) -> T:
        result = future.await_future(
            self.db, self.ss, f._future, time_limit_secs, presume_worker_dead_after_secs
        )
        match result:
            case future.AwaitFailed.TimeLimitExceeded:
                raise TimeoutError("Time limit exceeded")
            case future.AwaitFailed.WorkerPresumedDead:
                logging.warning(
                    "Worker responsible for this future died; attempting to resubmit and await again."
                )
                future.relinquish_claim(self.db, self.ss, f._future)
                # TODO: this is resetting our time limits. Should it?
                return self.await_future(
                    f, time_limit_secs, presume_worker_dead_after_secs
                )
            case future.AwaitFailed.FutureFailed:
                f = future.get_future_state(self.db, self.ss, f._future)
                match f:
                    case future.FailedFuture(latest_exception=e):
                        future.throw_future_exception(e)
                    case _:
                        raise Exception("Future failed with unknown exception.")
            case future.FutureResult(worker_id=worker_id, name=name):
                client = self._get_or_create_client(worker_id)
                get_result: T = client.get(name)
                return get_result


GLOBAL_CLIENT = None


def init(cluster_name: str) -> None:
    """Initialize global, shared connection to the cluster. This is required prior to
    using any other functions in this module.

    Advanced users can create a Client object explicitly."""
    global GLOBAL_CLIENT
    GLOBAL_CLIENT = Client(cluster_name)


def submit_future(
    fn: Callable[..., T],
    *args: Any,
    min_cpu: int = 0,
    min_ram: int = 0,
    min_gpu: int = 0
) -> Future[T]:
    """Submit a future to the cluster. Returns a Future object that can be used to
    await the result."""
    if GLOBAL_CLIENT is None:
        raise Exception("Must call fdb_ray_clone.init() before submitting futures.")
    return GLOBAL_CLIENT.submit_future(
        fn, *args, min_cpu=min_cpu, min_ram=min_ram, min_gpu=min_gpu
    )


def await_future(
    f: Future[T],
    time_limit_secs: Optional[int] = None,
    presume_worker_dead_after_secs: int = 30,
) -> T:
    """Await the result of a future. This blocks until the future is complete, or
    until the time limit is exceeded. If the time limit is exceeded, a TimeoutError
    is raised. If the worker responsible for the future is presumed dead (after
    having no heartbeat for presume_worker_dead_after_secs), the future
    is resubmitted and the await is retried."""
    if GLOBAL_CLIENT is None:
        raise Exception("Must call fdb_ray_clone.init() before submitting futures.")
    return GLOBAL_CLIENT.await_future(
        f, time_limit_secs, presume_worker_dead_after_secs
    )
