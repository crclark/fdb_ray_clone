from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple
import pytest
import uuid
from uuid import UUID

# import hypothesis
# import hypothesis.strategies as st
# from hypothesis.database import DirectoryBasedExampleDatabase
# from hypothesis.stateful import (
#     Bundle,
#     RuleBasedStateMachine,
#     rule,
#     run_state_machine_as_test,
#     initialize,
# )

import fdb_ray_clone.control.future as future

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
def nonexistent_future() -> future.UnclaimedFuture[int]:
    return future.UnclaimedFuture(
        code=lambda: 1,
        dependencies=[],
        resource_requirements=future.ResourceRequirements(),
        max_retries=3,
        id=UUID("00000000-0000-0000-0000-000000000000"),
    )


def test_create_get_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )

    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.UnclaimedFuture)
    assert f.code(*f.dependencies) == 3
    assert f.resource_requirements == future.ResourceRequirements()
    assert f.max_retries == 3


def test_claim_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )

    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.UnclaimedFuture)

    f_claim_ret = future.claim_future(db, subspace, f, "localhost", 1234)
    f_claim_state = future.get_future_state(db, subspace, f)
    get_claim_ret = future.get_future_claim(db, subspace, f.id)

    assert f_claim_ret.claim == f_claim_state.claim == get_claim_ret

    assert isinstance(f_claim_state, future.ClaimedFuture)
    assert f_claim_state.claim.worker_id.address == "localhost"
    assert f_claim_state.claim.worker_id.port == 1234
    assert get_claim_ret.worker_id.address == "localhost"
    assert get_claim_ret.worker_id.port == 1234


def test_claim_nonexistent_future(
    db: fdb.Database,
    subspace: fdb.Subspace,
    nonexistent_future: future.UnclaimedFuture[int],
) -> None:
    with pytest.raises(future.FutureDoesNotExistException):
        f = future.claim_future(db, subspace, nonexistent_future, "localhost", 1234)


def test_realize_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )

    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.UnclaimedFuture)

    f_claim_ret = future.claim_future(db, subspace, f, "localhost", 1234)
    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.ClaimedFuture)

    f = future.realize_future(db, subspace, f, "buffername")
    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.RealizedFuture)
    assert f.latest_result.worker_id.address == "localhost"
    assert f.latest_result.worker_id.port == 1234
    assert f.latest_result.name == "buffername"


def test_realize_stolen_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )
    future.claim_future(db, subspace, f, "localhost", 1234)
    f_orig_claim = future.get_future_state(db, subspace, f)

    future.claim_future(db, subspace, f, "localhost", 1235, force=True)
    with pytest.raises(Exception):
        f = future.realize_future(db, subspace, f_orig_claim, "buffername")


def test_realize_nonexistent_future(
    db: fdb.Database,
    subspace: fdb.Subspace,
    nonexistent_future: future.UnclaimedFuture[int],
) -> None:
    with pytest.raises(future.FutureDoesNotExistException):
        f = future.realize_future(db, subspace, nonexistent_future, "buffername")


def test_fail_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db,
        subspace,
        lambda x, y: x + y,
        [1, 2],
        future.ResourceRequirements(),
        max_retries=0,
    )
    f = future.claim_future(db, subspace, f, "localhost", 1234)

    f = future.fail_future(db, subspace, f, Exception("error message"))
    f = future.get_future_state(db, subspace, f)

    assert isinstance(f, future.FailedFuture)
    assert f.latest_exception is not None
    assert f.latest_exception.exception.__str__() == "error message"


def test_fail_stolen_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db,
        subspace,
        lambda x, y: x + y,
        [1, 2],
        future.ResourceRequirements(),
        max_retries=0,
    )
    f_orig_claim = future.claim_future(db, subspace, f, "localhost", 1234)

    future.claim_future(db, subspace, f, "localhost", 1235, force=True)
    with pytest.raises(future.ClaimLostException):
        f = future.fail_future(db, subspace, f_orig_claim, Exception("error message"))


def test_fail_nonexistent_future(
    db: fdb.Database,
    subspace: fdb.Subspace,
    nonexistent_future: future.UnclaimedFuture[int],
) -> None:
    with pytest.raises(future.FutureDoesNotExistException):
        f = future.fail_future(
            db, subspace, nonexistent_future, Exception("error message")
        )


def test_await_future(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )
    f = future.claim_future(db, subspace, f, "localhost", 1234)
    f = future.realize_future(db, subspace, f, "buffername")

    result = future.await_future(db, subspace, f)

    assert result is not None
    assert result.name == "buffername"


def test_await_future_timeout(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )

    assert (
        future.await_future(db, subspace, f, time_limit_secs=1)
        == future.AwaitFailed.TimeLimitExceeded
    )


def test_await_future_dead_worker(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )
    worker_id = future.WorkerId("localhost", 1234)
    f = future.claim_future(db, subspace, f, worker_id.address, worker_id.port)
    heartbeat = future.WorkerHeartbeat(
        last_heartbeat_at=0,
        started_at=0,
        available_resources=future.WorkerResources(cpu=1, ram=1, gpu=1),
    )
    future.write_worker_heartbeat(db, subspace, worker_id, heartbeat)

    result = future.await_future(
        db, subspace, f, time_limit_secs=10, presume_worker_dead_after_secs=1
    )

    assert result == future.AwaitFailed.WorkerPresumedDead


def test_await_future_live_worker(db: fdb.Database, subspace: fdb.Subspace) -> None:
    f = future.submit_future(
        db, subspace, lambda x, y: x + y, [1, 2], future.ResourceRequirements()
    )
    worker_id = future.WorkerId("localhost", 1234)
    f = future.claim_future(db, subspace, f, worker_id.address, worker_id.port)
    f = future.realize_future(db, subspace, f, "buffername")
    heartbeat = future.WorkerHeartbeat(
        last_heartbeat_at=future.seconds_since_epoch(),
        started_at=0,
        available_resources=future.WorkerResources(cpu=1, ram=1, gpu=1),
    )
    future.write_worker_heartbeat(db, subspace, worker_id, heartbeat)

    result = future.await_future(
        db, subspace, f, time_limit_secs=10, presume_worker_dead_after_secs=100
    )

    assert result is not None
    assert result.name == "buffername"


def test_scan_resource_requirements(db: fdb.Database, subspace: fdb.Subspace) -> None:
    n = 4
    futures = [
        future.submit_future(
            db,
            subspace,
            lambda: 1,
            [],
            future.ResourceRequirements(cpu=i, ram=i * 2),
            id=uuid.uuid4(),
        )
        for i in range(1, n + 1)
    ]
    available_resources = future.WorkerResources(cpu=3, ram=4, gpu=0)
    relevant_futures = future.futures_fitting_resources(
        db, subspace, available_resources
    )

    assert futures[0].id in relevant_futures
    assert futures[1].id in relevant_futures
    assert futures[2].id not in relevant_futures
    assert futures[3].id not in relevant_futures


def test_worker_heartbeat(db: fdb.Database, subspace: fdb.Subspace) -> None:
    worker_id = future.WorkerId("localhost", 1234)
    heartbeat = future.WorkerHeartbeat(
        last_heartbeat_at=future.seconds_since_epoch(),
        started_at=future.seconds_since_epoch(),
        available_resources=future.WorkerResources(1, 2, 3),
    )
    future.write_worker_heartbeat(
        db,
        subspace,
        worker_id,
        heartbeat,
    )

    ret_heartbeat = future.get_worker_heartbeat(db, subspace, worker_id)
    assert ret_heartbeat is not None
    assert ret_heartbeat == heartbeat


def test_all_worker_heartbeats(db: fdb.Database, subspace: fdb.Subspace) -> None:
    worker_ids = [future.WorkerId("localhost", i) for i in range(1, 4)]
    heartbeat = future.WorkerHeartbeat(
        last_heartbeat_at=future.seconds_since_epoch(),
        started_at=future.seconds_since_epoch(),
        available_resources=future.WorkerResources(1, 2, 3),
    )
    for worker_id in worker_ids:
        future.write_worker_heartbeat(
            db,
            subspace,
            worker_id,
            heartbeat,
        )

    heartbeats = future.all_worker_heartbeats(db, subspace)
    assert len(heartbeats) == 3
    for worker_id in worker_ids:
        assert worker_id in heartbeats
        assert heartbeats[worker_id] == heartbeat


# TODO: fix the state machine tests -- non-deterministic somehow

# @dataclass
# class FutureModel:
#     unclaimed: Set[UUID] = field(default_factory=set)
#     claimed: Set[UUID] = field(default_factory=set)
#     realized: Set[UUID] = field(default_factory=set)
#     failed: Set[UUID] = field(default_factory=set)

#     claims: Dict[UUID, Tuple[str, int]] = field(default_factory=dict)

#     # TODO: resource_requirements


# @st.composite
# def worker_strategy(
#     draw: st.DrawFn, elements: Any = st.integers(min_value=50000, max_value=55000)
# ) -> Any:
#     port = draw(elements)
#     ip = draw(st.ip_addresses())
#     return (ip, port)


# class FutureStateMachine(RuleBasedStateMachine):
#     def __init__(self, db: fdb.Database, subspace: fdb.Subspace) -> None:
#         super().__init__()
#         self.db = db
#         self.subspace = subspace
#         print(f"subspace: {subspace}")
#         self.model = FutureModel()

#     workers: Bundle[Tuple[str, int]] = Bundle("workers")
#     futures: Bundle[future.Future[int]] = Bundle("futures")

#     @initialize()
#     def clear_fdb(self) -> None:
#         self.db.clear_range(self.subspace.range().start, self.subspace.range().stop)

#     @rule(target=workers, worker=worker_strategy())
#     def create_worker(self, worker: Tuple[str, int]) -> Tuple[str, int]:
#         print(f"create worker: {worker}")
#         return worker

#     @rule(target=futures, f=st.uuids(version=4))  # type: ignore
#     def create_future(self, f: uuid.UUID) -> uuid.UUID:
#         print(f"create future: {f}")
#         return f

#     # TODO: precondition -- new futures only
#     @rule(f=futures)
#     def submit(self, f: uuid.UUID) -> None:
#         future.submit_future(
#             self.db,
#             self.subspace,
#             lambda x, y: x + y,
#             [1, 2],
#             future.ResourceRequirements(),
#             id=f,  # TODO: change this line to just f and mypy doesn't complain. Why not?
#         )
#         self.model.unclaimed.add(f)

#     @rule(worker=workers, f=futures)
#     def claim(self, worker: Tuple[str, int], f: uuid.UUID) -> None:
#         if f in self.model.unclaimed:
#             future.claim_future(self.db, self.subspace, f, worker)
#             self.model.unclaimed.remove(f)
#             self.model.claimed.add(f)
#             self.model.claims[f] = worker

#     @rule(f=futures)
#     def future_state_agrees(self, f: uuid.UUID) -> None:
#         fdb_future = future.get_future_state(self.db, self.subspace, f)
#         print(f"fdb_future: {fdb_future}, {type(fdb_future)}")
#         print(f"model: {self.model}")

#         # Check that model state and future state agree
#         if fdb_future is None:
#             assert f not in self.model.unclaimed
#             assert f not in self.model.claimed
#             assert f not in self.model.realized
#             assert f not in self.model.failed
#         match type(fdb_future):
#             case future.UnclaimedFuture:
#                 assert f in self.model.unclaimed
#                 assert f not in self.model.claimed
#                 assert f not in self.model.realized
#                 assert f not in self.model.failed
#             case future.ClaimedFuture:
#                 assert f not in self.model.unclaimed
#                 assert f in self.model.claimed
#                 assert f not in self.model.realized
#                 assert f not in self.model.failed
#             case future.RealizedFuture:
#                 assert f not in self.model.unclaimed
#                 assert f not in self.model.claimed
#                 assert f in self.model.realized
#                 assert f not in self.model.failed
#             case future.FailedFuture:
#                 assert f not in self.model.unclaimed
#                 assert f not in self.model.claimed
#                 assert f not in self.model.realized
#                 assert f in self.model.failed
#             case _:
#                 raise Exception("Future not found in model")

#     def teardown(self) -> None:
#         self.clear_fdb()


# def test_future_state_machine(db: fdb.Database, subspace: fdb.Subspace) -> None:
#     sm = FutureStateMachine(db, subspace)
#     settings = hypothesis.settings(
#         print_blob=True, verbosity=hypothesis.Verbosity.verbose
#     )
#     run_state_machine_as_test(lambda: sm, settings=settings)
