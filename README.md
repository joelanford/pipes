# pipes
Nanomsg pipelines using Etcd for service discovery

## Description
This is a very early version without all functionality.  So far, the `destination`, `group`, and `pipe` struct and member functions have been created.  I may or may not reorganize/rename these structs and their public functions

### `destination`
An instance of destination is created using `pipes.NewDestination(etcd_client, destination_name)`

The destination is registered (`destination.Register()`) by the sender, creating a directory in Etcd, "/streams/destinations/[name]".  Once a destination has been registered, several goroutines are started:
  - `maintainDestination()` - maintains the destination in Etcd (updates its TTL and re-creates the directory if it is deleted out from under the calling process).
  - `watchForGroups()` - watches Etcd for new group directories created in the destination directory. Sends responses on an interal watch response channel.
  - `handleGroupChanges()` - receives responses from `watchForGroups()` and creates/deletes pipes, to which receiver's can connect to receive data.

The destination can be unregistered (`destination.Unregister()`) to remove it from Etcd, which incidentally removes all joined groups

### `group`
An instance of group is created using `pipes.NewGroup(etcd_client, destination_name, group_name)`

A group is joined (`group.Join()`) by a receiver, creating a directory in Etcd, "/streams/destinations/[dest_name]/[group_name]/".  Once a group has been joined, a goroutine is started that maintains the group in Etcd (updates its TTL, waits for a destination to be re-created if it is removed, and re-creates the group if it is deleted out from under the calling process)

The group can be left (`group.Leave()`), which will stop the goroutine responsible for updating the TTL.  Note that leaving the group doesn't automatically delete it from Etcd.  In the case of multiple members joining the same group, the group directory will only expire from Etcd after all members have left the group.

## Examples
```
go run main/main.go
go run destination/main.go
go run group/main.go
```

## Known issues
1. If group.Leave() is called and the group.waitForDestination() function is running and waiting for the destination to be created, the call to group.Leave() will block until the watch returns (i.e. the destination is created), which isn't great. We need to add another channel and pass it to etcd's watch function, so that the call to group.Leave() can cancel this watch.
2. When destination.Register() is called, and the initial call to destination.createDirectory() returns an error, the program will block unless there's already another goroutine waiting for errors on the destination's RegistrationErrorChannel()
3. When group.Join() is called, and the initial calls to group.waitForDestination() or group.createGroup() return an error, the program will block unless there's already another goroutine waiting for errors on the group's JoinErrorChannel()
4. The pipe hardcodes its URL when it's created.  It should be passed to the NewPipe function.  How should we handle it in the destination code?  Pass a transport type, a bind address, and a port range?

## Next Steps
1. Fix the known issues
2. Add `destination_test.go` and `group_test.go`
3. Add goroutines to the `group.Join()` call to:
  - watch for additions, deletions, and changes to the group URL
  - connect to the socket when the URL is created, update the socket connection when the URL changes, and teardown the connection when the URL is deleted.
