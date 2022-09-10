from typing import Any, Union
import uuid
import pickle
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.shared_memory import SharedMemory
import pyarrow as pa
import pickle

PYARROW_WRITE_OPTIONS = pa.ipc.IpcWriteOptions(compression="lz4")
AUTH_KEY = b"not_very_secure"


class StoreServer(object):
    """Stores objects in local memory and serves them over the network to
    other processes.

    Intended to be used with `with` at worker startup, with one instance per worker
    under normal circumstances."""

    def __init__(self, bind_address: str, bind_port: int):
        self.smm = SharedMemoryManager((bind_address, bind_port), authkey=AUTH_KEY)
        self.smm.register("get", self.get)
        self.smm.register("get_used_ram", self.get_used_ram)
        self.used_ram = 0

    def __enter__(self) -> "StoreServer":
        self.smm.start()
        return self

    def __exit__(self, _exc_type: type, _exc_val: Any, _exc_tb: Any) -> None:
        # TODO: log exception, re-throw?
        self.smm.shutdown()

    def get(self, name: str) -> SharedMemory:
        return SharedMemory(name=name)

    def get_used_ram(self) -> int:
        return self.used_ram

    def _pyarrow_serialized_size_bytes(self, x: pa.Table) -> int:
        mock_sink = pa.MockOutputStream()

        with pa.RecordBatchStreamWriter(
            mock_sink, x.schema, options=PYARROW_WRITE_OPTIONS
        ) as stream_writer:
            for record_batch in x.to_batches():
                stream_writer.write_batch(record_batch)

        return int(mock_sink.size())

    def _store_local_pyarrow_table(self, x: pa.Table) -> SharedMemory:
        data_size = self._pyarrow_serialized_size_bytes(x)
        sm = self.smm.SharedMemory(size=data_size)
        self.used_ram += data_size
        stream = pa.FixedSizeBufferWriter(pa.py_buffer(sm.buf))
        stream_writer = pa.RecordBatchStreamWriter(
            stream, x.schema, options=PYARROW_WRITE_OPTIONS
        )
        for record_batch in x.to_batches():
            stream_writer.write_batch(record_batch)
        stream_writer.close()

        return sm

    def _store_picklable_object(self, x: Any) -> SharedMemory:
        pickled = pickle.dumps(x, protocol=5)
        sm = self.smm.SharedMemory(size=len(pickled))
        self.used_ram += len(pickled)
        sm.buf[:] = pickled
        return sm

    def store_local(self, x: Union[pa.Table, Any]) -> SharedMemory:
        """Stores x in a new byte buffer in the local machine's memory. Returns
        the SharedMemory object in which the object was stored."""

        # TODO: catch pickle errors and fall back to cloudpickle?
        if isinstance(x, pa.Table):
            return self._store_local_pyarrow_table(x)
        else:
            return self._store_picklable_object(x)

    def can_store(self, x: Union[pa.Table, Any]) -> bool:
        """Returns True if x can be passed to store_local, False otherwise."""
        if isinstance(x, pa.Table):
            return True
        try:
            pickle.dumps(x, protocol=5)
            return True
        except pickle.PicklingError:
            return False


class StoreClient(object):
    """Client for a StoreServer. Enables remote access to the objects stored in the
    StoreServer."""

    def __init__(self, address: str, port: int):
        self.smm = SharedMemoryManager(address=(address, port), authkey=AUTH_KEY)
        self.smm.register("get")
        self.smm.register("get_used_ram")
        self.smm.connect()

    def _deserialize(self, buf: bytes) -> Union[pa.Table, Any]:
        try:
            df_buffer = pa.BufferReader(pa.py_buffer(buf))
            reader = pa.RecordBatchStreamReader(df_buffer)
            return reader.read_all()
        except pa.ArrowInvalid as e:
            return pickle.loads(buf)

    # TODO: optimize for locality: if we are on the same machine as the server,
    # we can just return the SharedMemory object directly, rather than doing
    # a round trip over the network. We'll need to pass the WorkerConfig into
    # the StoreClient constructor to detect this case. Even better if we detect
    # this at a higher level and never even construct a StoreClient.
    def get(self, name: str) -> Union[pa.Table, Any]:
        # TODO: retries for transient network errors
        sm = self.smm.get(name)._getvalue()  # type: ignore [attr-defined]
        return self._deserialize(sm.buf)

    def get_used_ram(self) -> int:
        result: int = self.smm.get_used_ram()._getvalue()  # type: ignore [attr-defined]
        return result
