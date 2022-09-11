"""The future interface exposed to our worker process internals.
   For the user-facing future interface (which uses this interface internally),
   see the stuff in fdb_ray_clone's __init__.py.

   NOTE: the functions in this interface ONLY transition the FoundationDB state,
   NOT the data plane state. The worker logic is responsible for orchestrating
   the two.
"""


# TODO: idea: implement actors as stored results, add a new mutate_result function
# that pushes code to the worker that stores the actor, call the code on the
# actor, re-pickle the actor into the result buffer, and return the result.
# This breaks the immutability of the object store, but it seems like it could
# result in an elegant system where actors can be built on top of futures with
# very little additional code. OTOH, users might be surprised that their object
# gets pickled after every method call... if it has open file descriptors, etc.,
# that would break it.


from enum import Enum
import cloudpickle
from dataclasses import dataclass
import pickle
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    NoReturn,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)
from uuid import UUID
import uuid
import fdb
import time

fdb.api_version(710)

T = TypeVar("T")
U = TypeVar("U")


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


class FutureDoesNotExistException(Exception):
    pass


class ClaimLostException(Exception):
    pass


SecondsSinceEpoch = int


def seconds_since_epoch() -> SecondsSinceEpoch:
    return int(time.time())


@dataclass(frozen=True)
class WorkerId:
    address: str
    port: int


Resource = Union[Literal["cpu"], Literal["ram"], Literal["gpu"]]


@dataclass(frozen=True)
class ResourceRequirements:
    cpu: int = 0
    ram: int = 0
    gpu: int = 0


@dataclass(frozen=True)
class FutureClaim:
    worker_id: WorkerId
    claimed_at: SecondsSinceEpoch


# TODO: create a put_object function
# that doesn't require a corresponding future. Then get() can be used to get
# any object, regardless of whether it was created by a future. Document the
# downside: objects created by futures can be recovered by recomputing the
# future, objects created by put cannot be recovered.
@dataclass(frozen=True)
class ObjectRef(Generic[T]):
    timestamp: SecondsSinceEpoch
    worker_id: WorkerId
    name: str
    future_id: UUID


# TODO: function in client get_locality(future) -> Optional[LocalityRequirement]
# to get the locality info for a future if the future has been realized.
@dataclass(frozen=True)
class LocalityRequirement(Generic[T]):
    object_ref: ObjectRef[T]


Requirements = Union[ResourceRequirements, LocalityRequirement[T]]


@dataclass(frozen=True)
class FutureException:
    message: str


class RemoteException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def throw_future_exception(exception: FutureException) -> NoReturn:
    msg = "\n** Traceback from worker **:\n"
    raise RemoteException(msg + exception.message)


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
    args: List[Any]
    # no way to do existentials but we really don't care about U here, so
    # ignore the type.
    # https://stackoverflow.com/q/59621745
    requirements: Requirements[U]  # type: ignore [valid-type]
    max_retries: int


@dataclass(frozen=True)
class UnclaimedFuture(BaseFuture[T]):
    """A newly-submitted future that is not claimed by a worker. It could have
    been claimed in the past, though, so num_attempts could be non-zero."""

    num_attempts: int

    pass


@dataclass(frozen=True)
class ClaimedFuture(BaseFuture[T]):
    """A future which a worker has claimed for exclusive access to compute its
    result."""

    claim: FutureClaim
    num_attempts: int
    latest_exception: Optional[FutureException]


@dataclass(frozen=True)
class RealizedFuture(BaseFuture[T]):
    """A future for which a result has been successfully computed.
    The claim key is cleared because it is no longer being worked on."""

    num_attempts: int
    latest_exception: Optional[FutureException]
    latest_result: ObjectRef[T]


@dataclass(frozen=True)
class FailedFuture(BaseFuture[T]):
    """A future that was previously claimed, but exceeded its max retries."""

    num_attempts: int
    latest_exception: FutureException


# simple algebraic data type equivalent. Copilot even completed this for me.
# See http://blog.ezyang.com/2020/10/idiomatic-algebraic-data-types-in-python-with-dataclasses-and-union/
Future = Union[UnclaimedFuture[T], ClaimedFuture[T], RealizedFuture[T], FailedFuture[T]]


@dataclass(frozen=True)
class FutureFDBWatch(Generic[T]):
    fdb_success_watch: fdb.Future
    fdb_failure_watch: fdb.Future
    result_key: bytes


class AwaitFailed(Enum):
    TimeLimitExceeded = 1
    WorkerPresumedDead = 2
    FutureFailed = 3


FutureWatch = Union[AwaitFailed, FutureFDBWatch[T], ObjectRef[T]]


@fdb.transactional
def write_resource_requirements(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_id: UUID,
    requirements: ResourceRequirements,
) -> None:
    for resource in ["cpu", "ram", "gpu"]:
        resource_ss = ss.subspace((f"resource_requirements_{resource}",))
        req = requirements.__getattribute__(resource)
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
def scan_resource_requirements(
    tr: fdb.Transaction, ss: fdb.Subspace, resource: Resource, max_value: int
) -> Dict[UUID, int]:
    """Return all resource requirements for the given resource that are less
    than or equal to max_value. This can be used to find all resource requirements that
    are less than the available resources on a worker.

    Performed as a snapshot read to reduce spurious conflicts.

    Returns a dict where the keys are future ids and the values are requirements
    for the given resource."""
    resource_ss = ss.subspace((f"resource_requirements_{resource}",))
    end_key = resource_ss.pack((max_value + 1,))
    ret = dict()
    for key, _ in tr.snapshot.get_range(resource_ss.range().start, end_key):
        req, future_id = resource_ss.unpack(key)
        ret[future_id] = req
    return ret


@fdb.transactional
def futures_fitting_resources(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    available_resources: WorkerResources,
) -> List[UUID]:
    """Return a list of all futures that fit within the available resources."""
    ret: Set[UUID] = set()
    for resource in ["cpu", "ram", "gpu"]:
        reqs = scan_resource_requirements(
            tr, ss, resource, available_resources.__getattribute__(resource)
        )
        if not ret:
            ret.update(reqs.keys())
        else:
            ret.intersection_update(reqs.keys())
    return list(ret)


@fdb.transactional
def write_worker_assignment(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
    future_id: UUID,
    why: ObjectRef[T],
) -> None:
    assignment_subspace = ss.subspace(
        ("worker_assignments", worker_id.address, worker_id.port)
    )
    tr[assignment_subspace[future_id]] = pickle.dumps(why)


@fdb.transactional
def clear_worker_assignment(
    tr: fdb.Transaction, ss: fdb.Subspace, worker_id: WorkerId, future_id: UUID
) -> None:
    assignment_subspace = ss.subspace(
        ("worker_assignments", worker_id.address, worker_id.port)
    )
    del tr[assignment_subspace[future_id]]


@fdb.transactional
def iterate_worker_assignments(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
    fn: Callable[[fdb.Transaction, UUID, ObjectRef[Any]], Optional[T]],
) -> Optional[T]:
    """Iterate over all the futures assigned to the given worker, and call fn
    on each one. If fn returns a non-None value, then that value is returned
    and iteration stops. Otherwise, None is returned."""
    assignment_subspace = ss.subspace(
        ("worker_assignments", worker_id.address, worker_id.port)
    )
    for key, value in tr.get_range(
        assignment_subspace.range().start, assignment_subspace.range().stop
    ):
        result = fn(tr, assignment_subspace.unpack(key)[0], pickle.loads(value))
        if result is not None:
            return result
    return None


@fdb.transactional
def get_all_assignments(
    tr: fdb.Transaction, ss: fdb.Subspace, worker_id: WorkerId
) -> List[Tuple[UUID, ObjectRef[T]]]:
    ret = []
    iterate_worker_assignments(tr, ss, worker_id, lambda tr, k, v: ret.append((k, v)))
    return ret


def write_requirements(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_id: UUID,
    requirements: Requirements[T],
) -> None:
    match requirements:
        case ResourceRequirements():
            write_resource_requirements(tr, ss, future_id, requirements)
        case LocalityRequirement(object_ref):
            write_worker_assignment(tr, ss, object_ref.worker_id, future_id, object_ref)
        case [LocalityRequirement(object_ref), *rest]:
            write_worker_assignment(tr, ss, object_ref.worker_id, future_id, object_ref)
        case _:
            raise Exception(f"Unknown requirement type: {type(requirements)}")


def clear_requirements(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_id: UUID,
    requirements: Requirements[T],
) -> None:
    match requirements:
        case ResourceRequirements():
            clear_resource_requirements(tr, ss, future_id, requirements)
        case LocalityRequirement(object_ref):
            clear_worker_assignment(tr, ss, object_ref.worker_id, future_id)


@fdb.transactional
def _set_worker_claim_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
    future_id: UUID,
) -> None:
    worker_ss = ss.subspace((f"workers",))
    tr[worker_ss[worker_id.address][worker_id.port]["claim_future"]] = pickle.dumps(
        future_id
    )


@fdb.transactional
def _clear_worker_claim_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
) -> None:
    worker_ss = ss.subspace((f"workers",))
    del tr[worker_ss[worker_id.address][worker_id.port]["claim_future"]]


@fdb.transactional
def get_worker_claim_future(
    tr: fdb.Transaction, ss: fdb.Subspace, worker_id: WorkerId
) -> Optional[UUID]:
    """Returns the UUID of the future that the given worker currently has a claim on."""
    worker_ss = ss.subspace((f"workers",))
    return unpickle(
        tr[worker_ss[worker_id.address][worker_id.port]["claim_future"]].wait()
    )


@fdb.transactional
def write_worker_heartbeat(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
    heartbeat: WorkerHeartbeat,
) -> None:
    worker_ss = ss.subspace((f"workers",))
    # Reading a somewhat stale heartbeat is okay.
    tr.options.set_next_write_no_write_conflict_range()
    tr[worker_ss.pack((worker_id.address, worker_id.port, "heartbeat"))] = pickle.dumps(
        heartbeat
    )


@fdb.transactional
def get_worker_heartbeat(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    worker_id: WorkerId,
) -> Optional[WorkerHeartbeat]:
    worker_ss = ss.subspace((f"workers",))
    val = tr[worker_ss.pack((worker_id.address, worker_id.port, "heartbeat"))]
    return unpickle(val)


@fdb.transactional
def all_worker_heartbeats(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
) -> Dict[WorkerId, WorkerHeartbeat]:
    worker_ss = ss.subspace((f"workers",))
    ret: Dict[WorkerId, WorkerHeartbeat] = dict()
    for key, val in tr.get_range(worker_ss.range().start, worker_ss.range().stop):
        match worker_ss.unpack(key):
            case [address, port, "heartbeat"]:
                ret[WorkerId(address, port)] = pickle.loads(val)
            case _:
                continue  # ignore non-heartbeat keys
    return ret


@fdb.transactional
def submit_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future_code: Callable[..., T],
    args: List[Any],
    requirements: Union[ResourceRequirements, LocalityRequirement[T]],
    max_retries: int = 3,
    id: Optional[UUID] = None,
) -> UnclaimedFuture[T]:
    """Submit a future to the cluster.

    Parameters:

    future_code -- the function to execute
    args -- Python values that will be passed to the future as arguments.
      These must be small in size -- use futures if you need to refer to a large
      result of another future.
    requirements -- the resources required to execute this future.
      These can either be CPU, RAM, and GPU requirements, or a locality requirement.
      A locality requirement indicates that the future must be executed on the
      same worker as another completed future.
    max_retries -- the maximum number of times to retry this future if it fails.
    id -- the UUID to use for this future. If not provided, a random UUID will be
      generated.
    """
    # TODO: throw exception if args are huge after pickling, instead of
    # letting the transaction fail.
    id = id or uuid.uuid4()
    future = UnclaimedFuture(
        id=id,
        code=future_code,
        args=args,
        requirements=requirements,
        max_retries=max_retries,
        num_attempts=0,
    )
    future_ss = ss.subspace(("future", id))
    tr[future_ss.pack(("code",))] = cloudpickle.dumps(future_code)
    tr[future_ss.pack(("args",))] = cloudpickle.dumps(args)
    tr[future_ss.pack(("requirements",))] = pickle.dumps(requirements)
    tr[future_ss.pack(("max_retries",))] = pickle.dumps(max_retries)

    write_requirements(tr, ss, future.id, requirements)
    return future


@fdb.transactional
def resubmit_realized_future_bad_object_ref(
    tr: fdb.Transaction, ss: fdb.Subspace, future_id: UUID, bad_object_ref: ObjectRef[T]
) -> bool:
    """Resubmit a future for recomputation by another worker if its object reference is broken (i.e., the worker is
    unreachable, or the worker no longer contains the object).

    Returns True if the future was successfully resubmitted. Returns False if the provided object reference is
    out-of-date, which means that someone else has already resubmitted the future."""
    future_ss = ss.subspace(("future", future_id))
    current_object_ref = unpickle(tr[future_ss["latest_result"]].wait())

    if current_object_ref != bad_object_ref:
        # The caller's information is out of date; do nothing.
        return False
    else:
        # The caller's information is up-to-date; resubmit the future.
        del tr[future_ss["latest_result"]]
        del tr[future_ss["num_attempts"]]
        requirements = unpickle(tr[future_ss["requirements"]].wait())
        if requirements:
            write_requirements(tr, ss, future_id, requirements)
        else:
            raise Exception("Impossible happened: future has no requirements")
        return True


@fdb.transactional
def future_exists(tr: fdb.Transaction, ss: fdb.Subspace, id: UUID) -> bool:
    future_ss = ss.subspace(("future", id))
    result: bool = tr.get(future_ss.pack(("code",))).wait().present()
    return result


@fdb.transactional
def get_future_claim(
    tr: fdb.Transaction, ss: fdb.Subspace, id: UUID
) -> Optional[FutureClaim]:
    """Returns the claim for the given future, or None if the future is unclaimed or does not exist."""
    future_ss = ss.subspace(("future", id))
    claim = tr[future_ss["claim"]].wait()
    return unpickle(claim)


@fdb.transactional
def get_future_state(  # type: ignore [return] # TODO: wtf
    tr: fdb.Transaction, ss: fdb.Subspace, future: Union[UUID, BaseFuture[T]]
) -> Optional[Future[T]]:
    """Get the state of a future, identified either by its UUID or a future
    object. Returns None if the future does not exist in FoundationDB."""

    future_id = future if isinstance(future, UUID) else future.id
    future_ss = ss.subspace(("future", future_id))

    # keys we need in order to know which case to return (unclaimed, etc.)
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
    args = tr[future_ss["args"]]
    requirements = tr[future_ss["requirements"]]
    latest_exception = tr[future_ss["latest_exception"]]

    if not future_exists(tr, ss, future_id):
        return None

    match (
        claim,
        latest_result,
        num_attempts,
        max_retries,
    ):
        case (
            None,
            None,
            num_attempts,
            max_retries,
        ) if num_attempts >= max_retries + 1:
            return FailedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                args=must_unpickle(args.wait()),
                requirements=must_unpickle(requirements.wait()),
                max_retries=max_retries,
                num_attempts=num_attempts,
                latest_exception=must_unpickle(latest_exception.wait()),
            )
        case (None, None, num_attempts, max_retries):
            return UnclaimedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                args=must_unpickle(args.wait()),
                requirements=must_unpickle(requirements.wait()),
                max_retries=max_retries,
                num_attempts=num_attempts,
            )

        case (FutureClaim(), None, num_attempts, max_retries):
            return ClaimedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                args=must_unpickle(args.wait()),
                requirements=must_unpickle(requirements.wait()),
                max_retries=max_retries,
                claim=claim,
                num_attempts=num_attempts,
                latest_exception=unpickle(latest_exception.wait()),
            )
        case (None, ObjectRef(), num_attempts, max_retries):
            return RealizedFuture(
                id=future_id,
                code=must_unpickle(code.wait()),
                args=must_unpickle(args.wait()),
                requirements=must_unpickle(requirements.wait()),
                max_retries=max_retries,
                num_attempts=num_attempts,
                latest_result=latest_result,
                latest_exception=unpickle(latest_exception.wait()),
            )
        case _:
            raise Exception(
                f"Unexpected case: {(claim, latest_result, num_attempts, max_retries)}"
            )


# TODO: we also need a more general resubmit_future that can transition a future
# from any state to unclaimed, in case an object has been lost by worker death.
@fdb.transactional
def relinquish_claim(
    tr: fdb.Transaction, ss: fdb.Subspace, future: ClaimedFuture[T]
) -> UnclaimedFuture[T]:
    """Clears the claim for a future, transitioning it back to the unclaimed state
    so that a worker can claim it again. Fails with an exception if the input
    claim is not the current claim for the future."""

    future_ss = ss.subspace(("future", future.id))
    latest_claim = unpickle(tr[future_ss["claim"]].wait())
    if not latest_claim or latest_claim != future.claim:
        raise ClaimLostException(
            f"Cannot relinquish outdated claim. Latest claim is {latest_claim}, but input claim is {future.claim}."
        )
    del tr[future_ss["claim"]]
    _clear_worker_claim_future(tr, ss, future.claim.worker_id)
    write_requirements(tr, ss, future.id, future.requirements)
    return UnclaimedFuture(
        id=future.id,
        code=future.code,
        args=future.args,
        requirements=future.requirements,
        max_retries=future.max_retries,
        num_attempts=future.num_attempts,
    )


# pedagogical note: using more precise types to encode valid transitions in
# the future state machine. Might ultimately be useless, though.
@fdb.transactional
def claim_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future: Union[UUID, UnclaimedFuture[T]],  # TODO: replace all Union with |
    worker_address: str,
    worker_port: int,
    force: bool = False,
) -> ClaimedFuture[T]:
    """Claim a future for the given worker. If force=True and the future is
    already claimed,
    that claim is overwritten and invalidated. This also resets the
    num_attempts counter (under the assumption that the new worker may be
    in a better state than the previous worker attempting to work on the
    future -- e.g., more memory)."""
    future_id = future if isinstance(future, UUID) else future.id

    if not future_exists(tr, ss, future_id):
        raise FutureDoesNotExistException(future_id)

    unclaimed_future: Future[T] = get_future_state(tr, ss, future_id)
    if not force and not isinstance(unclaimed_future, UnclaimedFuture):
        raise Exception(f"Future {future_id} is not unclaimed: {unclaimed_future}")

    clear_requirements(tr, ss, unclaimed_future.id, unclaimed_future.requirements)
    worker_id = WorkerId(worker_address, worker_port)
    _set_worker_claim_future(tr, ss, worker_id, future_id)
    claim = FutureClaim(
        worker_id=worker_id,
        claimed_at=seconds_since_epoch(),
    )
    future_ss = ss.subspace(("future", future_id))
    tr[future_ss["claim"]] = pickle.dumps(claim)
    return ClaimedFuture(
        id=future_id,
        code=unclaimed_future.code,
        args=unclaimed_future.args,
        requirements=unclaimed_future.requirements,
        max_retries=unclaimed_future.max_retries,
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
    future_result: ObjectRef[T] = ObjectRef(
        timestamp=seconds_since_epoch(),
        worker_id=worker_id,
        name=name,
        future_id=future.id,
    )
    tr[future_ss["latest_result"]] = pickle.dumps(future_result)
    # TODO: blind writes are brittle
    tr[future_ss["num_attempts"]] = pickle.dumps(future.num_attempts + 1)
    _clear_worker_claim_future(tr, ss, future.claim.worker_id)
    del tr[future_ss["claim"]]
    return RealizedFuture(
        id=future.id,
        code=future.code,
        args=future.args,
        requirements=future.requirements,
        max_retries=future.max_retries,
        num_attempts=future.num_attempts + 1,
        latest_result=future_result,
        latest_exception=latest_exception,
    )


@fdb.transactional
def fail_future(
    tr: fdb.Transaction,
    ss: fdb.Subspace,
    future: ClaimedFuture[T],
    exception_message: str,
    fail_permanently: bool = False,
) -> Union[FailedFuture[T], ClaimedFuture[T]]:
    """Records an exception that occurred while running the future's code, and
       increments num_attempts on the future.
    Returns a FailedFuture if the future has exceeded its max_retries.
    Throws if the future has been claimed by another worker.

    Keyword arguments:
    fail_permanently -- if True, set num_attempts to max_retries + 1, so that
    the future will be marked as failed permanently."""

    if not future_exists(tr, ss, future.id):
        raise FutureDoesNotExistException(future.id)

    future_exception = FutureException(message=exception_message)
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

    if fail_permanently or future.num_attempts >= future.max_retries:
        # clear the claim and return failure object.
        _clear_worker_claim_future(tr, ss, future.claim.worker_id)
        del tr[future_ss["claim"]]
        return FailedFuture(
            id=future.id,
            code=future.code,
            args=future.args,
            requirements=future.requirements,
            max_retries=future.max_retries,
            num_attempts=future.max_retries + 1,
            latest_exception=future_exception,
        )
    else:
        return ClaimedFuture(
            id=future.id,
            code=future.code,
            args=future.args,
            requirements=future.requirements,
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
    result: Optional[ObjectRef[T]] = unpickle(tr[future_ss["latest_result"]].wait())
    exception: Optional[FutureException] = unpickle(
        tr[future_ss["latest_exception"]].wait()
    )
    if result:
        return result
    elif exception:
        return AwaitFailed.FutureFailed
    else:
        success_key = future_ss["latest_result"]
        failure_key = future_ss["latest_exception"]
        return FutureFDBWatch(
            fdb_success_watch=tr.watch(success_key),
            fdb_failure_watch=tr.watch(failure_key),
            result_key=success_key,
        )


@fdb.transactional
def _get_future_watch_result(
    tr: fdb.Transaction, future_watch: FutureFDBWatch[T]
) -> ObjectRef[T]:
    result: Optional[ObjectRef[T]] = unpickle(tr.get(future_watch.result_key).wait())
    if result:
        return result
    else:
        raise Exception("Watch triggered but no result found.")


@fdb.transactional
def _future_worker_latest_heartbeat(
    tr: fdb.Transaction, ss: fdb.Subspace, future_id: UUID
) -> Optional[int]:
    """Returns the timestamp of the latest heartbeat for the worker that is
    currently working on the future. Returns None if no worker is working on it."""
    claim = get_future_claim(tr, ss, future_id)
    if claim:
        worker_id = claim.worker_id
        heartbeat = get_worker_heartbeat(tr, ss, worker_id)
        if not heartbeat:
            raise Exception(
                f"Worker {worker_id} has claimed a future but has no heartbeat."
            )
        else:
            last_heartbeat_at: int = heartbeat.last_heartbeat_at
            return last_heartbeat_at
    else:
        return None


def _await_future_watch(
    db: fdb.Database,
    ss: fdb.Subspace,
    future_watch: FutureWatch[T],
    future_id: UUID,
    time_limit_secs: Optional[int] = None,
    presume_worker_dead_after_secs: Optional[int] = None,
) -> Union[AwaitFailed, ObjectRef[T]]:
    """Blocks until the future has been realized and returns its result. If time limit is exceeded, returns None."""
    if isinstance(future_watch, FutureFDBWatch):
        started_at = seconds_since_epoch()
        last_worker_heartbeat_at = None
        curr_poll_wait = 1
        max_poll_wait = 16
        while True:
            wait_duration = seconds_since_epoch() - started_at
            if future_watch.fdb_success_watch.is_ready():
                result: ObjectRef[T] = _get_future_watch_result(db, future_watch)
                return result
            elif future_watch.fdb_failure_watch.is_ready():
                return AwaitFailed.FutureFailed
            elif time_limit_secs is not None and wait_duration > time_limit_secs:
                return AwaitFailed.TimeLimitExceeded
            elif (
                presume_worker_dead_after_secs is not None
                and wait_duration > presume_worker_dead_after_secs
            ):
                last_worker_heartbeat_at = _future_worker_latest_heartbeat(
                    db, ss, future_id
                )
                if (
                    last_worker_heartbeat_at is not None
                    and seconds_since_epoch() - last_worker_heartbeat_at
                    > presume_worker_dead_after_secs
                ):
                    return AwaitFailed.WorkerPresumedDead
            else:
                time.sleep(curr_poll_wait)
                curr_poll_wait = min(curr_poll_wait * 2, max_poll_wait)
    else:
        return future_watch


def await_future(
    db: fdb.Database,
    ss: fdb.Subspace,
    future: Future[T],
    time_limit_secs: Optional[int] = None,
    presume_worker_dead_after_secs: Optional[int] = None,
) -> Union[AwaitFailed, ObjectRef[T]]:
    """Blocks until the future has been realized and returns its result.
    If time limit is exceeded, returns AwaitFailed.TimeLimitExceeded.
    If presume_worker_dead_after_secs is provided, returns AwaitFailed.WorkerPresumedDead
    if we haven't seen a heartbeat from the worker that claimed the future within that time.
    This logic only takes effect after the future has been claimed."""
    return _await_future_watch(
        db,
        ss,
        _create_future_watch(db, ss, future),
        future.id,
        time_limit_secs,
        presume_worker_dead_after_secs,
    )


@fdb.transactional
def await_all(
    tr: fdb.Transaction, ss: fdb.Subspace, futures: List[Future[T]]
) -> List[Future[T]]:
    pass


@fdb.transactional
def delete_future(tr: fdb.Transaction, ss: fdb.Subspace, future: Future[T]) -> None:
    pass
