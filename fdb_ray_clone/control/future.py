"""The future interface exposed to our worker process internals.
   For the user-facing future interface (which uses this interface internally),
   see the stuff in fdb_ray_clone's __init__.py.

   NOTE: the functions in this interface ONLY transition the FoundationDB state,
   NOT the data plane state. The worker logic is responsible for orchestrating
   the two."""

import cloudpickle
from dataclasses import dataclass
import pickle
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union
from uuid import UUID
import uuid
import fdb
import time

fdb.api_version(710)

T = TypeVar("T")


def unpickle(x: "fdb.Value") -> Optional[T]:
    if not x.present():
        return None
    else:
        result: T = pickle.loads(x.value)
        return result


def must_unpickle(x: "fdb.Value") -> T:
    if not x.present():
        raise Exception("Value not present")
    else:
        result: T = pickle.loads(x.value)
        return result


def add(x: int, y: int) -> int:
    return x + y


class FutureDoesNotExistException(Exception):
    pass


class ClaimLostException(Exception):
    pass


"""
FDB data model

**All subspaces are contained within a subspace scoped by a user-provided
cluster name.**

# futures subspace

Contains a list of all futures known to the system.

## Invariants

- A future is in one of these mutually exclusive and exhaustive states:
  - Unclaimed (and unrealized) -- `claim` key is not set
  - Claimed (and unrealized) -- `claim` key is set and `latest_result` is not
                                 set or has an earlier timestamp than the claim.
  - Realized -- `claim` key is set and `latest_result` is set, comes from the
                  same worker as the claim, and has a later timestamp than the
                  claim.
  - Failed -- num_attempts > max_retries + 1
- A future can only be claimed by one worker at a time.
- If a future is unclaimed, it is present in the resource_requirements_* secondary
  index subspaces.
- If a future is claimed, its claim key is set.
- A future can be moved from claimed to unclaimed without being realized in two
  ways:
  - The worker that claimed the future voluntarily relinquishes the claim.
  - The worker that claimed the future dies permanently and another worker detects
    the situation and relinquishes the claim on behalf of the dead worker.
    # TODO: figure out how this detection will work.
- A future can be moved from realized to unrealized and unclaimed if a worker
  detects that all stored copies of the future's result have been lost by
  worker crashes. This resets num_attempts (#TODO: should it?).

## Key/values per future:

- key (`future_id`, 'code'): value: cloudpickled function to run to produce
  the future's result.
- key (`future_id`, 'latest_result'): value: pickled FutureResult object.
  If this key is not present, the future
  has not yet been realized. For a full list of results of this future, see
  the results subspace.
- key (`future_id`, 'dependencies'): value: pickled list of FutureParam objects.
  These are the dependencies of the future, which are passed as parameters to
  the future's function when it runs. If this key is not present, the
  future has no dependencies.
- key (`future_id`, 'num_attempts'): value: pickled int. The number of times
  a worker has attempted to realize a future. If this value is greater than one,
  a worker has encountered an exception while trying to realize the future, or
  has crashed for unrelated reasons (the latter can be inferred by a missing
  exception key; see below).
- key (`future_id`, 'max_retries'): value: pickled int. The number of times
  to retry realizing the future before giving up. If num_attempts >= max_retries,
  the future will never be realized.
- key (`future_id`, 'latest_exception'): value: pickled FutureException object. The exception
  string of the last failure. If this key is not present, the future has never
  failed. If this key is present, the future has failed in the past, but may
  have succeeded since then.
- key (`future_id`, 'resource_requirements'): value: ResourceRequirements object. The
  resources required to realize the future. If this key is not present, any
  worker can realize the future. The values here are kept in sync with the
  resource_requirements_cpu, resource_requirements_ram, and resource_requirements_gpu
  subspaces, which are secondary indices into the futures subspace.
- key (`future_id`, 'claim'): value: pickled FutureClaim object. The claim on the
  future. If this key is not present, the future has not been claimed. If this
  key is present, the future has been claimed and a worker may be working on
  the future (or the worker may have died). See error handling below for more
  details.

# resource_requirements_* subspace

Contains a list of all futures that require a certain resource. This is ordered
so that workers can easily filter for futures that they have enough resources
to realize. This is a secondary index into the futures subspace.

# TODO: locality requirements or hints. I think we could just do a key of
# (worker_id, future_id) and an empty value. I think we can assume that each
# future can have a locality requirement on at most one result, but that result
# can be replicated to multiple workers, so we'd have multiple redundant locality
# hints. We'd need to be careful of transaction conflicts when workers try to
# claim futures with a locality hint, because we'd be more likely to have
# conflicts when we are more or less asking two workers to fight over a future.

## Invariants

- All unclaimed futures are included in these secondary indices. If a worker
  relinquishes its claim on a future without realizing it, it must reinsert
  the future into these indices. See the relinquish_claim method for more info.
- Futures are removed from this index when claimed. See the claim_future method.
  This index is the primary means of claiming futures.
- If a future doesn't explicitly require a resource, its requirement will be
  set to 0 and it will be included in this secondary index.
- It is acceptable to read ranges of these indices as snapshot reads, so long
  as claim operations are not snapshot reads. This is because we want only one
  worker to claim a future.

## Key/values per future:

- key (`resource_type`, `resource_requirement`, `future_id`): value: None.

# results subspace

Contains a list of all future results known to the system. For each future,
we list all of its results. There may be more than one if the worker originally
containing the future crashed, or if the user requested the future result to
be cloned to another worker. # TODO: implement API for cloning future results.

## Invariants

- If a result exists, the future has a non-null latest_result key.

## Key/values per result:

- key (`future_id`, versionstamp): value: pickled FutureResult object. This contains the
  timestamp at which the result was realized, the worker id, and the name of
  the shared memory object containing the result. **If the timestamp is older
  than the started_at timestamp of the corresponding worker, the object is no
  longer available (the worker has since crashed and restarted). Workers that
  notice this situation should delete this key/value pair.**

# workers subspace

Contains information on all workers that are members of the cluster.

Workers are uniquely identified by IP address and port. A worker that has crashed
and restarted with the same IP address and port is considered the same worker,
though it may no longer contain the realized futures it used to.


## Key/values per worker:

- key (`worker_ip`, `worker_port`): value: pickled WorkerHeartbeat object. This
  contains the timestamp of the worker's last heartbeat, the timestamp at which
  the worker started up, and the worker's
  available resources (CPU is constant, RAM is adjusted to account for free space,
  since the worker is storing future results).

# Error handling and failure modes

The following failures can occur in the future lifecycle:

- The worker that claimed the future has died and restarts.
- The worker that claimed the future has died and does not restart.
- The future code threw an exception.
- No worker has the required resources to realize the future. In this case, no
  worker will attempt to claim the future, so it will never fail and never
  succeed. We should set keys containing the max resources across all workers
  and fail to submit the future if its requirements exceed it. But what if
  the worker with the max resources subsequently dies? In that case, we should
  have workers periodically set a heartbeat key in a workers subspace, which
  all workers periodically poll. Then we can error out if no live worker can
  fulfill the future's requirements.
- A future the future depends on has failed permanently (max_retries exceeded).
- The worker fails to deserialize the future code.
- The worker that claimed the future doesn't have enough memory to fulfill the future,
  but retries are always retry on the same worker. Retrying on another worker
  could succeed.
- A future's serialized code exceeds the value size limit of FoundationDB.

# TODO: idea: implement actors as stored results, add a new mutate_result function
# that pushes code to the worker that stores the actor, call the code on the
# actor, re-pickle the actor into the result buffer, and return the result.
# This breaks the immutability of the object store, but it seems like it could
# result in an elegant system where actors can be built on top of futures with
# very little additional code. OTOH, users might be surprised that their object
# gets pickled after every method call... if it has open file descriptors, etc.,
# that would break it.

# TODO: instead of workers looking for work, should the worker who creates the
# future be responsible for assigning it to a worker? Then we'd just need a
# task queue for each worker. If a worker fails, though, how would a future get
# reassigned? Of course, that's also an open question for the current design. I
# guess that a worker could set a timeout for futures it is awaiting, compare that
# to the heartbeat of the worker that claimed the future, and reassign it if the
# worker has died. This would also allow us to fail fast if a resource requirement
# is unfulfillable (do we want to? What if it's temporary, as in the case of memory?).

# TODO: API to delete a future and its results. We need this to be able to
# repartition a dataset and clean up the old partitions.

# TODO: API to transfer ownership of a future's result to another worker. This
# is also needed for repartitioning.

# TODO: API to put something into the object store locally and register it as a
# realized future. This is kinda equivalent to `return` in a monadic future API.

# TODO: reference counting and garbage collection.

How repartitioning should work:

for each current partition:
  new_partitions = split_into_n_parts(current_partition, n)
  new_partition_futures = []
  transfer_ownership_futures = []
  for new_partition in new_partitions:
    realized_future = put_realized_future(new_partition)
    new_partition_futures.append(realized_future)
    transfer_ownership_future = future(lambda: take_ownership(realized_future), resources={'ram': realized_future.size_in_bytes})
    transfer_ownership_futures.append(transfer_ownership_future)
  await_all(transfer_ownership_futures)
  delete_all(transfer_ownership_futures) # boring cleanup since we don't have GC yet
  delete_future(current_partition)


# TODO: API to delete a result without deleting the future?

"""

SecondsSinceEpoch = int


def seconds_since_epoch() -> SecondsSinceEpoch:
    return int(time.time())


@dataclass(frozen=True)
class WorkerId:
    address: str
    port: int


@dataclass(frozen=True)
class ResourceRequirements:
    cpu: int = 0
    ram: int = 0
    gpu: int = 0


@dataclass(frozen=True)
class FutureClaim:
    worker_id: WorkerId
    claimed_at: SecondsSinceEpoch


@dataclass(frozen=True)
class FutureResult(Generic[T]):
    timestamp: SecondsSinceEpoch
    worker_id: WorkerId
    name: str


@dataclass(frozen=True)
class FutureException:
    # optional because some exceptions are not picklable.
    exception: Optional[Exception]
    exception_type: type
    # traceback: Optional[str] # TODO


# TODO: worker must calculate its available memory as the difference between
# what it currently has in its store and its configured max memory. If it were
# to use free system memory, that could lead to oversubscription if multiple
# workers are running on the same machine. It also allows users to intentionally
# oversubscribe if they want to use swap.
@dataclass(frozen=True)
class WorkerResources:
    cpu: int
    ram: int
    gpu: int


@dataclass(frozen=True)
class WorkerHeartbeat:
    last_heartbeat_at: SecondsSinceEpoch
    started_at: SecondsSinceEpoch
    available_resources: WorkerResources


@dataclass(frozen=True)
class BaseFuture(Generic[T]):
    id: UUID
    code: Callable[..., T]
    dependencies: List[Any]
    resource_requirements: ResourceRequirements
    max_retries: int


@dataclass(frozen=True)
class UnclaimedFuture(BaseFuture[T]):
    pass


@dataclass(frozen=True)
class ClaimedFuture(BaseFuture[T]):
    claim: FutureClaim
    num_attempts: int
    latest_exception: Optional[FutureException]


@dataclass(frozen=True)
class RealizedFuture(ClaimedFuture[T]):
    latest_result: FutureResult[T]


@dataclass(frozen=True)
class FailedFuture(ClaimedFuture[T]):
    pass


# simple algebraic data type equivalent. Copilot even completed this for me.
# See http://blog.ezyang.com/2020/10/idiomatic-algebraic-data-types-in-python-with-dataclasses-and-union/
Future = Union[UnclaimedFuture[T], ClaimedFuture[T], RealizedFuture[T], FailedFuture[T]]


@dataclass(frozen=True)
class FutureFDBWatch(Generic[T]):
    fdb_watch: fdb.Future
    key: bytes


FutureWatch = Union[FutureFDBWatch[T], FutureResult[T]]


@fdb.transactional
def write_resource_requirements(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_id: UUID,
    resource_requirements: ResourceRequirements,
) -> None:
    for resource in ["cpu", "ram", "gpu"]:
        resource_ss = ss.subspace((f"resource_requirements_{resource}",))
        req = resource_requirements.__getattribute__(resource)
        tr[resource_ss.pack((req, future_id))] = b""


@fdb.transactional
def clear_resource_requirements(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_id: UUID,
    resource_requirements: ResourceRequirements,
) -> None:
    for resource in ["cpu", "ram", "gpu"]:
        resource_ss = ss.subspace((f"resource_requirements_{resource}",))
        req = resource_requirements.__getattribute__(resource)
        del tr[resource_ss.pack((req, future_id))]


@fdb.transactional
def submit_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_code: Callable[..., T],
    dependencies: List[Any],
    resource_requirements: ResourceRequirements,
    max_retries: int = 3,
    id: Optional[UUID] = None,
) -> UnclaimedFuture[T]:
    # TODO: throw exception if dependencies are huge after pickling, instead of
    # letting the transaction fail.
    id = id or uuid.uuid4()
    future = UnclaimedFuture(
        id=id,
        code=future_code,
        dependencies=dependencies,
        resource_requirements=resource_requirements,
        max_retries=max_retries,
    )
    future_ss = ss.subspace(("future", id))
    tr[future_ss.pack(("code",))] = cloudpickle.dumps(future_code)
    tr[future_ss.pack(("dependencies",))] = cloudpickle.dumps(dependencies)
    tr[future_ss.pack(("resource_requirements",))] = pickle.dumps(resource_requirements)
    tr[future_ss.pack(("max_retries",))] = pickle.dumps(max_retries)

    write_resource_requirements(tr, ss, future.id, resource_requirements)
    return future


@fdb.transactional
def future_exists(tr: fdb.Transaction, ss: fdb.Subspace, id: UUID) -> bool:
    future_ss = ss.subspace(("future", id))
    result: bool = tr.get(future_ss.pack(("code",))).wait().present()
    return result


@fdb.transactional
def get_future_state(  # type: ignore [return] # TODO: wtf
    tr: fdb.Transaction, ss: fdb.Subspace, future: Union[UUID, BaseFuture[T]]
) -> Optional[Future[T]]:
    """Get the state of a future, identified either by its UUID or a future
    object. Returns None if the future does not exist in FoundationDB."""

    future_id = future if isinstance(future, UUID) else future.id
    future_ss = ss.subspace(("future", future_id))

    # keys we need to be able to decide which case to return (unclaimed, etc.)
    claim = tr[future_ss["claim"]]
    latest_result = tr[future_ss["latest_result"]]
    num_attempts = tr[future_ss["num_attempts"]]
    max_retries = tr[future_ss["max_retries"]]
    claim = unpickle(claim.wait())
    latest_result = unpickle(latest_result.wait())
    num_attempts = unpickle(num_attempts.wait()) or 0
    max_retries = unpickle(max_retries.wait())

    # The remaining keys
    code = tr[future_ss["code"]]
    dependencies = tr[future_ss["dependencies"]]
    resource_requirements = tr[future_ss["resource_requirements"]]
    latest_exception = tr[future_ss["latest_exception"]]

    if not future_exists(tr, ss, future_id):
        return None

    match (
        claim,
        latest_result,
        num_attempts,
        max_retries,
    ):
        case (None, _, _, max_retries):
            return UnclaimedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                dependencies=must_unpickle(dependencies.wait()),
                resource_requirements=must_unpickle(resource_requirements.wait()),
                max_retries=max_retries,
            )
        case (
            claim,
            None,
            num_attempts,
            max_retries,
        ) if num_attempts >= max_retries + 1:
            return FailedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                dependencies=must_unpickle(dependencies.wait()),
                resource_requirements=must_unpickle(resource_requirements.wait()),
                max_retries=max_retries,
                claim=claim,
                num_attempts=num_attempts,
                latest_exception=must_unpickle(latest_exception.wait()),
            )

        case (claim, None, num_attempts, max_retries):
            return ClaimedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                dependencies=must_unpickle(dependencies.wait()),
                resource_requirements=must_unpickle(resource_requirements.wait()),
                max_retries=max_retries,
                claim=claim,
                num_attempts=num_attempts or 0,
                latest_exception=unpickle(latest_exception.wait()),
            )
        case (claim, latest_result, num_attempts, max_retries):
            return RealizedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                dependencies=must_unpickle(dependencies.wait()),
                resource_requirements=must_unpickle(resource_requirements.wait()),
                max_retries=max_retries,
                claim=claim,
                num_attempts=num_attempts,
                latest_result=latest_result,
                latest_exception=unpickle(latest_exception.wait()),
            )
        case _:
            raise Exception(
                f"Unexpected case: {(claim, latest_result, num_attempts, max_retries)}"
            )


# pedagogical note: using more precise types to encode valid transitions in
# the future state machine. Might ultimately be useless, though.
@fdb.transactional
def claim_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future: UnclaimedFuture[T],
    worker_address: str,
    worker_port: int,
) -> ClaimedFuture[T]:
    """Claim a future for the given worker. If the future is already claimed,
    that claim is overwritten and invalidated. This also resets the
    num_attempts counter (under the assumption that the new worker may be
    in a better state than the previous worker attempting to work on the
    future -- e.g., more memory)."""

    if not future_exists(tr, ss, future.id):
        raise FutureDoesNotExistException(future.id)

    clear_resource_requirements(tr, ss, future.id, future.resource_requirements)
    claim = FutureClaim(
        worker_id=WorkerId(worker_address, worker_port),
        claimed_at=seconds_since_epoch(),
    )
    future_ss = ss.subspace(("future", future.id))
    tr[future_ss["claim"]] = pickle.dumps(claim)
    return ClaimedFuture(
        id=future.id,
        code=future.code,
        dependencies=future.dependencies,
        resource_requirements=future.resource_requirements,
        max_retries=future.max_retries,
        claim=claim,
        num_attempts=0,
        latest_exception=None,
    )


@fdb.transactional
def realize_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future: ClaimedFuture[T],
    name: str,
) -> RealizedFuture[T]:
    """
    The worker calls this function when it has successfully computed the result
    of the future.
    Throws without recording the result if the worker's claim on the future has
    expired.
    """

    if not future_exists(tr, ss, future.id):
        raise FutureDoesNotExistException(future.id)

    worker_id = future.claim.worker_id
    future_ss = ss.subspace(("future", future.id))
    latest_claim = unpickle(tr[future_ss["claim"]].wait())
    latest_exception = unpickle(tr[future_ss["latest_exception"]].wait())
    # Copilot reminded me to write this condition :)
    if not latest_claim or latest_claim != future.claim:
        raise ClaimLostException(
            f"Worker {worker_id} lost claim on Future {future.id} while working on it. Current claim is {latest_claim}."
        )
    future_result: FutureResult[T] = FutureResult(
        timestamp=seconds_since_epoch(),
        worker_id=worker_id,
        name=name,
    )
    tr[future_ss["latest_result"]] = pickle.dumps(future_result)
    # TODO: blind writes are brittle
    tr[future_ss["num_attempts"]] = pickle.dumps(future.num_attempts + 1)
    return RealizedFuture(
        id=future.id,
        code=future.code,
        dependencies=future.dependencies,
        resource_requirements=future.resource_requirements,
        max_retries=future.max_retries,
        claim=future.claim,
        num_attempts=future.num_attempts + 1,
        latest_result=future_result,
        latest_exception=latest_exception,
    )


@fdb.transactional
def fail_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future: ClaimedFuture[T],
    exception: Exception,
) -> Union[FailedFuture[T], ClaimedFuture[T]]:
    """Records an exception that occurred while running the future's code, and
       increments num_attempts on the future.
    Returns a FailedFuture if the future has exceeded its max_retries.
    Throws if the future has been claimed by another worker."""

    if not future_exists(tr, ss, future.id):
        raise FutureDoesNotExistException(future.id)

    # convert exception to FutureException and store it in the future
    try:
        pickle.dumps(exception)
        future_exception = FutureException(
            exception=exception, exception_type=exception.__class__
        )
    except pickle.PicklingError:
        future_exception = FutureException(
            exception=None, exception_type=exception.__class__
        )
    future_ss = ss.subspace(("future", future.id))
    latest_claim = unpickle(tr[future_ss["claim"]].wait())
    if not latest_claim or latest_claim != future.claim:
        raise ClaimLostException(
            f"Worker {future.claim.worker_id} lost claim on Future {future.id} while working on it. Current claim is {latest_claim}."
        )
    future_ss = ss.subspace(("future", future.id))
    tr[future_ss["latest_exception"]] = pickle.dumps(future_exception)
    # TODO: blind writes are brittle
    tr[future_ss["num_attempts"]] = pickle.dumps(future.num_attempts + 1)

    if future.num_attempts + 1 >= future.max_retries:
        return FailedFuture(
            id=future.id,
            code=future.code,
            dependencies=future.dependencies,
            resource_requirements=future.resource_requirements,
            max_retries=future.max_retries,
            claim=future.claim,
            num_attempts=future.num_attempts + 1,
            latest_exception=future_exception,
        )
    else:
        return ClaimedFuture(
            id=future.id,
            code=future.code,
            dependencies=future.dependencies,
            resource_requirements=future.resource_requirements,
            max_retries=future.max_retries,
            claim=future.claim,
            num_attempts=future.num_attempts + 1,
            latest_exception=future_exception,
        )


@fdb.transactional
def _create_future_watch(
    tr: fdb.Transaction, ss: fdb.Subspace, future: Future[T]
) -> FutureWatch[T]:
    """Registers a watch with FDB on the result of the future. Must be awaited
    after the transaction has been committed."""
    future_ss = ss.subspace(("future", future.id))
    result: Optional[FutureResult[T]] = unpickle(tr[future_ss["latest_result"]].wait())
    if result:
        return result
    else:
        key = future_ss.pack(("latest_result",))
        return FutureFDBWatch(fdb_watch=tr.watch(key), key=key)


@fdb.transactional
def _get_future_watch_result(
    tr: fdb.Transaction, future_watch: FutureFDBWatch[T]
) -> FutureResult[T]:
    result: Optional[FutureResult[T]] = unpickle(tr.get(future_watch.key).wait())
    if result:
        return result
    else:
        raise Exception("Watch triggered but no result found.")


def _await_future_watch(
    db: fdb.Database,
    future_watch: FutureWatch[T],
    time_limit_secs: Optional[int] = None,
) -> Optional[FutureResult[T]]:
    """Blocks until the future has been realized and returns its result. If time limit is exceeded, returns None."""
    if isinstance(future_watch, FutureFDBWatch):
        started_at = seconds_since_epoch()
        while True:
            if future_watch.fdb_watch.is_ready():
                result: FutureResult[T] = _get_future_watch_result(db, future_watch)
                return result
            elif (
                time_limit_secs is not None
                and seconds_since_epoch() - started_at > time_limit_secs
            ):
                return None
            else:
                time.sleep(1)
    else:
        return future_watch


def await_future(
    db: fdb.Database,
    ss: fdb.Subspace,
    future: Future[T],
    time_limit_secs: Optional[int] = None,
) -> Optional[FutureResult[T]]:
    """Blocks until the future has been realized and returns its result. If time limit is exceeded, returns None."""
    return _await_future_watch(
        db, _create_future_watch(db, ss, future), time_limit_secs
    )


@fdb.transactional
def await_all(
    tr: fdb.Transaction, ss: fdb.Subspace, futures: List[Future[T]]
) -> List[Future[T]]:
    pass


@fdb.transactional
def delete_future(tr: fdb.Transaction, ss: fdb.Subspace, future: Future[T]) -> None:
    pass
