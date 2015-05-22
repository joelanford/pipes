package main;

import (
  "github.com/coreos/go-etcd/etcd"
  "pipes"
  "time"
  "log"
)

func main() {
  machines := []string{"http://127.0.0.1:4001"}
  client := etcd.NewClient(machines)

  dest := pipes.NewDestination(client, "foo")
  group := pipes.NewGroup(client, "foo", "bar1")
  dest.Register(true)
  log.Println("Registered destination")
  group.Join(true)
  log.Println("Joined group")

  go monitorChannels(dest.ErrorChannel(), group.ErrorChannel())

  time.Sleep(8*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")

  time.Sleep(3*time.Second)
  dest.Register(true)
  log.Println("Registered destination")

  time.Sleep(8*time.Second)
  group.Leave()
  log.Println("Left group")

  time.Sleep(3*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")
}

func monitorChannels(destRegisterErrorChan, groupJoinErrorChan <-chan error) {
  go func() {
    for {
      select {
      case err := <-destRegisterErrorChan:
        log.Println("Destination error: " + err.Error())
      case err := <-groupJoinErrorChan:
        log.Println("Group error: " + err.Error())
      }
    }
  }()
}
