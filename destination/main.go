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
  dest.Register(true)
  log.Println("Registered destination")

  go monitorChannels(dest.RegistrationErrorChannel(), dest.WatchGroupErrorChannel(), dest.HandleGroupChangesErrorChannel())

  time.Sleep(8*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")

  time.Sleep(3*time.Second)
  dest.Register(true)
  log.Println("Registered destination")

  time.Sleep(16*time.Second)
  dest.Unregister()
  log.Println("Unregistered destination")
}

func monitorChannels(destRegisterErrorChan, destGroupWatchErrorChan, destGroupHandleErrorChan <-chan error) {
  go func() {
    for {
      select {
      case err := <-destRegisterErrorChan:
        log.Println("Destination error: " + err.Error())
      case err := <-destGroupWatchErrorChan:
        log.Println("Destination Group Watch error: " + err.Error())
      case err := <-destGroupHandleErrorChan:
        log.Println("Destination Group Handling error: " + err.Error())
      }
    }
  }()
}
