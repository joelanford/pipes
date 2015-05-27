package main;

import (
  "github.com/coreos/go-etcd/etcd"
  "github.com/joelanford/pipes"
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

  go monitorChannels(dest.RegistrationErrorChannel(), dest.WatchGroupErrorChannel(), dest.HandleGroupChangesErrorChannel(), group.JoinErrorChannel())

  time.Sleep(8*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")

  time.Sleep(3*time.Second)
  dest.Register(true)
  log.Println("Registered destination")

  time.Sleep(time.Second)
  dest.Send([]byte("Hello, World!\n"))
  dest.Send([]byte("Hola, World!\n"))
  dest.Send([]byte("Bonjour, World!\n"))

  time.Sleep(8*time.Second)
  group.Leave()
  log.Println("Left group")

  time.Sleep(3*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")
}

func monitorChannels(destRegisterErrorChan, destGroupWatchErrorChan, destGroupHandleErrorChan, groupJoinErrorChan <-chan error) {
  go func() {
    for {
      select {
      case err := <-destRegisterErrorChan:
        log.Println("Destination Registration error: " + err.Error())
      case err := <-destGroupWatchErrorChan:
        log.Println("Destination Group Watch error: " + err.Error())
      case err := <-destGroupHandleErrorChan:
        log.Println("Destination Group Handling error: " + err.Error())
      case err := <-groupJoinErrorChan:
        log.Println("Group error: " + err.Error())
      }
    }
  }()
}
