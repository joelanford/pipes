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

  group := pipes.NewGroup(client, "foo", "bar1")
  group.Join(true)
  log.Println("Joined group")

  go monitorChannels(group.JoinErrorChannel())

  time.Sleep(16*time.Second)
  group.Leave()
  log.Println("Left group")
}

func monitorChannels(groupJoinErrorChan <-chan error) {
  go func() {
    for {
      select {
      case err := <-groupJoinErrorChan:
        log.Println("Group error: " + err.Error())
      }
    }
  }()
}
