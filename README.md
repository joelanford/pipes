# pipes
Nanomsg pipelines using Etcd for service discovery

## Description
This is a very early version without all functionality (i.e. no nanomsg connections are being created or registered in etcd).  So far, the `destination` and `group` struct and member functions have been created. 

### `destination`
An instance of destination is created using `pipes.NewDestination(etcd_client, destination_name)`

The destination is registered (`destination.Register()`) by the sender, creating a directory in Etcd, "/streams/destinations/[name]".  Once a destination has been registered, a goroutine is started that maintains the destination in Etcd (updates its TTL and re-creates the directory if it is deleted out from under the calling process.

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

## Next Steps
1. Add `destination_test.go` and `group_test.go`
2. Add goroutines to the `destination.Register()` call to:
  - watch for group additions and deletions
  - create a socket and register a URL under the group directory when a group is created, delete the socket when a group expires
  
3. Add goroutines to the `group.Join()` call to:
  - watch for additions, deletions, and changes to the group URL
  - connect to the socket when the URL is created, update the socket connection when the URL changes, and teardown the connection when the URL is deleted.
