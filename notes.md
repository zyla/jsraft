# Implementing Raft

## Abstractions

Raft depends on some facilities, which are out of scope of the algorithm itself, namely: network transport and storage.

It is a good idea to maintain this distinction in code, i.e. implement core Raft against an abstract interface to both network and storage. When doing so, one must decide at which level to define the abstraction.

Both storage and transport require encoding. Should "transport" be concerned with only transmissing opaque blobs, or maybe it should know what types and structures of messages Raft uses?

On the one hand, working with opaque blobs makes the transport implementation much smaller, and less coupled to Raft. This choice also makes it possible to extend Raft with additional message types without changing the transport implementation.

On the other hand, some message types may require special treatment for better performance. This is more obvious with the storage abstraction - the Raft log is a potentially very big data structure, and implementing it using opaque blob storage makes little sense.

There are also testing considerations. Tests may run much faster if we don't have encode and decode messages.
