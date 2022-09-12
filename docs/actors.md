Actors are objects running on a worker. When you call methods on actors, the return value is a future that can be awaited to get the result of the method. Method outputs are stored in the object store like any other object.

# Implementation

Actors are simply stored in a dict in the `StoreServer` class on each worker. Method calls to actors are simply futures with a locality requirement specifying the worker which contains the actor. This means that, unlike simple objects in the object store, actors are not accessible via the SharedMemoryManager server running on each worker. This allows actors to be implemented with minimal additional code on top of futures.

## Data plane

Actors are stored in the `self.actors` variable inside `StoreServer`. This is a dictionary where the keys are Future ids (of the future that created the actor) and the values are the actors themselves. The `store_actor()` method is used to store an actor. Actors are accessed for method calls directly from the `self.actors` dictionary.

Unfortunately, the memory usage of an actor can't be tracked by the store's memory usage statistics, so the `used_ram` variable will be an underestimate.

## Control plane

The future system is almost entirely unaware of actors. The primary change made to support actors is to extend the `ObjectRef` type to be a union of `ActorRef | BufferRef`, and to make the `object_id` param of `realize_future` signal that the future's result is an actor or a buffer. We also needed to add a flag `allow_reconstruction` to `Future` and set it to `False` for the results of actor method calls -- because actors are stateful, simply calling the same method on the same actor again won't necessarily produce the same result. This obviously can be true of the results of other futures, too, but this seems to be the correct default behavior for actors specifically.

Behind the scenes, the public API to create actors and make calls to them just submits futures.

## FAQ

- Why aren't actors just stored in buffers like other objects in the object store? Because we'd like actors to be allowed to create threads, file handles, and other things that can't be serialized. We also don't want to block object store server threads while waiting for method calls to complete, so we can't use it for method calls, anyway.
