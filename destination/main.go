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
  dest.Register(true)
  log.Println("Registered destination")

  go monitorChannels(dest.ErrorChannel())

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

func monitorChannels(destRegisterErrorChan <-chan error) {
  go func() {
    for {
      select {
      case err := <-destRegisterErrorChan:
        log.Println("Destination error: " + err.Error())
      }
    }
  }()
}
