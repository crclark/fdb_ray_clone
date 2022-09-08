# Intro

A very very minimal distributed futures system written in pure Python, vaguely inspired by Ray.

## Quick start

### Install FoundationDB

https://apple.github.io/foundationdb/downloads.html

### Start a worker

`poetry run python fdb_ray_clone/worker.py --address=127.0.0.1 --port=50001`

## Data plane

Each worker process is running a Python [SharedMemoryManager](https://docs.python.org/3/library/multiprocessing.shared_memory.html#multiprocessing.managers.SharedMemoryManager), which is a lightweight way to share memory between Python processes over a network. The hello world of SharedMemoryManager examples crashes in my system Python [because of a bug](https://stackoverflow.com/questions/59172691/why-do-we-get-a-nameerror-when-trying-to-use-the-sharedmemorymanager-python-3-8), so this requires Python >3.8.

The SharedMemoryManager configuration **assumes a private network.** `authkey` is set to a constant string.

Each shared memory buffer contains a Python object. There is an optimized write path for PyArrow that uses lz4 compression.

Disk spill is handled transparently by Linux swap. Create a swap file if you want to use it.

# TODOs

- Implement actors. Method calls on actors could be futures with locality constraints. Actors themselves could either be stored in buffers and deserialized/reserialized on every call (easier, but limiting), or stored separately in the Store.
- Brag about how few deps we have.
