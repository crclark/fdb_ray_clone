# Intro

A very very minimal distributed futures system written in pure Python, vaguely inspired by Ray.

## Data plane

Each worker process is running a Python [SharedMemoryManager](https://docs.python.org/3/library/multiprocessing.shared_memory.html#multiprocessing.managers.SharedMemoryManager), which is a lightweight way to share memory between Python processes over a network. The hello world of SharedMemoryManager examples crashes in my system Python [because of a bug](https://stackoverflow.com/questions/59172691/why-do-we-get-a-nameerror-when-trying-to-use-the-sharedmemorymanager-python-3-8), so this requires Python >3.8.

The SharedMemoryManager configuration **assumes a private network.** `authkey` is set to a constant string.

Each shared memory buffer contains a Python object. There is an optimized write path for PyArrow that uses lz4 compression.

Disk spill is handled transparently by Linux swap. Create a swap file if you want to use it.

# TODOs

- Remove grpc dependency and generated code. We don't need it because we have SharedMemoryManager.
- Brag about how few deps we have.
