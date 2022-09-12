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
)
from dataclasses import dataclass
import logging
import multiprocessing.managers

import fdb_ray_clone.control.future as future
import fdb_ray_clone.data.store as store

import fdb

fdb.api_version(710)

T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True)
class Future(Generic[T]):
    _future: future.Future[T]


class WorkerDiedException(Exception):
    pass


class UpstreamDependencyFailedException(Exception):
    pass


class UpstreamDependencyFailedNoReconstructionAllowed(Exception):
    pass


@dataclass(frozen=True)
class Actor(Generic[T]):
    actor_ref: future.ActorRef[T]


class Client(object):
    def __init__(self, cluster_name: str):
        self.ss = fdb.Subspace(("fdb_ray_clone", cluster_name))
        self.db = fdb.open()
        self.clients: Dict[future.WorkerId, store.StoreClient] = dict()

    def _destroy_all_cluster_state(self) -> None:
        """Wipes all distributed future/actor data in FoundationDB.
        For debugging/testing only."""
        self.db.clear_range(self.ss.range().start, self.ss.range().stop)

    def submit_future(
        self,
        fn: Callable[..., T],
        # TODO: fix args so that we can pass args and kwargs correctly.
        *args: Any,
        min_cpu: int = 0,
        min_ram: int = 0,
        min_gpu: int = 0,
        locality: Optional[future.LocalityRequirement[U]] = None,
    ) -> Future[T]:
        resources = (
            locality
            if locality
            else future.ResourceRequirements(min_cpu, min_ram, min_gpu)
        )
        return Future(
            _future=future.submit_future(self.db, self.ss, fn, args, resources)
        )

    def _get_or_create_client(self, worker_id: future.WorkerId) -> store.StoreClient:
        if worker_id not in self.clients:
            self.clients[worker_id] = store.StoreClient(
                worker_id.address, worker_id.port
            )
        return self.clients[worker_id]

    # TODO resource reqs/locality
    def create_actor(self, ctor: Callable[..., T], *args: Any) -> Future[Actor[T]]:
        def _mk_actor(*args: Any) -> store.Actor[T]:
            actor = ctor(*args)
            return store.Actor(actor)

        f: Future[Actor[T]] = Future(
            _future=future.submit_future(
                self.db,
                self.ss,
                _mk_actor,
                args,
                requirements=future.ResourceRequirements(),
            ),
        )

        return f

    def call_actor(
        self, actor_ref: Actor[T], method_name: str, *args: Any
    ) -> Future[U]:
        def _call_actor(
            actor_ref: future.ActorRef[T], method_name: str, *args: Any
        ) -> U:
            assert isinstance(store.WORKER_STORE_SERVER, store.StoreServer)
            actor = store.WORKER_STORE_SERVER.actors[actor_ref.future_id]
            result: U = getattr(actor, method_name)(*args)
            return result

        locality = future.LocalityRequirement(actor_ref.actor_ref)
        return Future(
            _future=future.submit_future(
                self.db,
                self.ss,
                _call_actor,
                (actor_ref.actor_ref, method_name, *args),
                requirements=locality,
            )
        )

    @fdb.transactional
    def _attempt_resubmit(self, tr: fdb.Transaction, f: Future[T]) -> bool:
        """Attempt to resubmit a future that has failed.

        Returns True if the future was successfully resubmitted, False if the
        future was already claimed by another worker.
        """
        f = future.get_future_state(tr, self.ss, f._future)
        match f:
            case future.ClaimedFuture():
                future.relinquish_claim(self.db, self.ss, f._future)
                return True
            case _:
                # Someone else already fixed the problem; we're done.
                return False

    def _resubmit_bad_object_ref(
        self, tr: fdb.Transaction, f: Future[T], object_ref: future.ObjectRef[T]
    ) -> None:
        """Resubmit a future that has failed due to a bad object ref."""
        future.resubmit_realized_future_bad_object_ref(
            tr, self.ss, f._future.id, object_ref
        )

    @fdb.transactional
    def _locality(
        self, tr: fdb.Transaction, f: Future[T]
    ) -> List[future.LocalityRequirement[T]]:
        future_state = future.get_future_state(tr, self.ss, f._future)
        match future_state:
            case future.RealizedFuture(latest_result=latest_result):
                return [future.LocalityRequirement(latest_result)]
            case _:
                return []

    def locality(self, f: Future[T]) -> List[future.LocalityRequirement[T]]:
        """Get the list of workers that contain the results of this future, if any.
        Returns an empty list if the future is not yet completed, or failed."""
        ret: List[future.LocalityRequirement[T]] = self._locality(self.db, f)
        return ret

    def await_future(  # type: ignore [return] # TODO: mypy false positive?
        self,
        f: Future[T],
        time_limit_secs: Optional[int] = None,
        presume_worker_dead_after_secs: int = 30,
        allow_resubmit: bool = True,
    ) -> T:
        """Wait for a future to complete and return its result.

        Keyword arguments:
         time_limit_secs -- If set, raise an exception if the future is not
             completed within this many seconds.
         presume_worker_dead_after_secs -- If set, assume that the worker
             fulfilling this future is
             dead if it has not updated its heartbeat in this many seconds.
             If this occurs, the future will either be resubmitted for
             computation by another worker, or an exception will be raised,
             depending on allow_resubmit is True.
         allow_resubmit -- If True, resubmit the future for computation if
             the worker fulfilling it is presumed dead. If False, raise an
             exception if the worker is presumed dead.
        """
        result = future.await_future(
            self.db, self.ss, f._future, time_limit_secs, presume_worker_dead_after_secs
        )
        match result:
            case future.AwaitFailed.TimeLimitExceeded:
                raise TimeoutError("Time limit exceeded")
            case future.AwaitFailed.WorkerPresumedDead if allow_resubmit:
                logging.warning(
                    "Worker responsible for this future died; attempting to resubmit and await again."
                )
                self._attempt_resubmit(self.db, f)
                # TODO: this is resetting our time limits. Should it?
                return self.await_future(
                    f, time_limit_secs, presume_worker_dead_after_secs
                )
            case future.AwaitFailed.WorkerPresumedDead if not allow_resubmit:
                raise WorkerDiedException()
            case future.AwaitFailed.FutureFailed:
                f = future.get_future_state(self.db, self.ss, f._future)
                match f:
                    case future.FailedFuture(latest_exception=e):
                        future.throw_future_exception(e)
                    case _:
                        raise Exception("Future failed with unknown exception.")
            case future.ActorRef() as actor_ref:
                ret: T = Actor(actor_ref)  # type: ignore [assignment]
                return ret
            case future.BufferRef(worker_id=worker_id, buffer_name=name) as buffer_ref:
                client = self._get_or_create_client(worker_id)
                # TODO: this can throw various exceptions if the worker is dead.
                # ConnectionRefusedError, timeouts, etc.
                try:
                    get_result: T = client.get(name)
                    return get_result
                except multiprocessing.managers.RemoteError:
                    # The remote store no longer has the object, which means
                    # the worker died and restarted. The future needs to be
                    # resubmitted.
                    if allow_resubmit:
                        logging.warning(
                            "Worker responsible for this future died; attempting to resubmit and await again."
                        )
                        # The worker died and restarted and no longer has the result
                        # in its store. We need to resubmit the future.
                        self._resubmit_bad_object_ref(self.db, f, buffer_ref)
                        return self.await_future(
                            f, time_limit_secs, presume_worker_dead_after_secs
                        )
                    else:
                        raise WorkerDiedException()


GLOBAL_CLIENT = None


def init(cluster_name: str) -> None:
    """Initialize global, shared connection to the cluster. This is required prior to
    using any other functions in this module.

    Advanced users can create a Client object explicitly."""
    global GLOBAL_CLIENT
    GLOBAL_CLIENT = Client(cluster_name)


# TODO: lineage reconstruction for the easy case where there are no locality
# requirements.


def submit_future(
    fn: Callable[..., T],
    *args: Any,
    min_cpu: int = 0,
    min_ram: int = 0,
    min_gpu: int = 0,
    locality: Optional[future.LocalityRequirement[U]] = None,
) -> Future[T]:
    """Submit a future to the cluster. Returns a Future object that can be used to
    await the result."""
    if GLOBAL_CLIENT is None:
        raise Exception(
            "Must call fdb_ray_clone.client.init() before submitting futures."
        )
    return GLOBAL_CLIENT.submit_future(
        fn, *args, min_cpu=min_cpu, min_ram=min_ram, min_gpu=min_gpu, locality=locality
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
        raise Exception(
            "Must call fdb_ray_clone.client.init() before submitting futures."
        )
    return GLOBAL_CLIENT.await_future(
        f, time_limit_secs, presume_worker_dead_after_secs
    )


def locality(f: Future[T]) -> List[future.LocalityRequirement[T]]:
    """Get the list of workers that contain the results of this future, if any.
    Returns an empty list if the future is not yet completed, or failed."""
    if GLOBAL_CLIENT is None:
        raise Exception(
            "Must call fdb_ray_clone.client.init() before submitting futures."
        )
    return GLOBAL_CLIENT.locality(f)


def create_actor(ctor: Callable[..., T], *args: Any) -> Future[Actor[T]]:
    """Create an actor on the cluster. Returns a Future that can be used to await
    the actor handle."""
    if GLOBAL_CLIENT is None:
        raise Exception(
            "Must call fdb_ray_clone.client.init() before submitting futures."
        )
    return GLOBAL_CLIENT.create_actor(ctor, *args)


def call_actor(actor: Actor[T], method_name: str, *args: Any) -> Future[U]:
    """Call a method on an actor. Returns a Future that can be used to await
    the result."""
    if GLOBAL_CLIENT is None:
        raise Exception(
            "Must call fdb_ray_clone.client.init() before submitting futures."
        )
    return GLOBAL_CLIENT.call_actor(actor, method_name, *args)
