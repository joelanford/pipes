package pipes

import (
  "github.com/coreos/go-etcd/etcd"
  "sync"
  "time"
)

type destination struct {
  // etcd is the distributed store for service discovery
  client *etcd.Client

  // The name of the destination
  name string

  // The etcd key of the destination
  key string

  // Channel to signal goroutines to end
  done chan bool

  // Channel to send errors
  errChan chan error

  // Used to wait for goroutines to finish
  wg sync.WaitGroup
}

func NewDestination(client *etcd.Client, name string) *destination {
    d := &destination{
      client: client,
      name: name,
      key: "/streams/destinations/" + name,
      done: make(chan bool),
      errChan: make(chan error),
    }
    return d
}

func (d *destination) Register(reregister bool) {
  // Create destination in etcd
  if err := d.createDestination(); err != nil {
    d.errChan <- err
  }

  // Launch goroutine for updating the destination TTL value
  d.wg.Add(1)
  go d.maintainDestination(reregister)

  // TODO: Launch 2 goroutines to 1. watch destination directory for new groups
  //                              2. handle events about group creation/deletion
  //  - When new group is created, create a new socket and add url key to group,
  //  - When group is deleted, tear down socket (url will be gone automatically)
}

func (d *destination) Unregister() {
  d.done <- true
  d.wg.Wait()
}

func (d destination) ErrorChannel() <-chan error {
  return d.errChan
}

func (d *destination) maintainDestination(reregister bool) {
  defer d.wg.Done()
  for {
    select {
    case <- d.done:
        // when a signal arrives on this channel, the Unregister method has
        // been called, so we'll delete the destination and exit this
        // goroutine
        if err := d.deleteDestination(); err != nil {
          d.errChan <- err
        }
        return
    default:
      time.Sleep(1* time.Second)
      if err := d.refreshDestinationTtl(2); err != nil {
        e := err.(*etcd.EtcdError)
        // If we lost connection to etcd, send the error, but keep attempting
        // future updates
        if e.ErrorCode == 501 {
            d.errChan <- err

        // Else, if the destination directory is not present, we'll either:
        //   - if automatically re-registerting, attempt to re-register
        //   - if not automatically re-registering, send the error and return
        } else if e.ErrorCode == 100 {
          if reregister == true {
            if err := d.createDestination(); err != nil {
              d.errChan <- err
            }
          } else {
            d.errChan <- err
            return
          }
        // Else, this is an unexpected error, so we'll let the caller know.
        } else {
          d.errChan <- err
        }
      }
    }
  }
}

func (d destination) refreshDestinationTtl(ttl uint64) error {
  if _, err := d.client.UpdateDir(d.key, ttl); err != nil {
    return err
  }
  return nil
}

func (d destination) createDestination() error {
  if _, err := d.client.CreateDir(d.key, 2); err != nil {
    // In contrast to the group, we should return an error if the destination
    // already exists, because only one process can send to a destination.
    // TODO: Do we want to allow multiple destination senders?  Do pipelines
    // work with both multiple senders and multiple receivers?
    return err
  }
  return nil
}

func (d destination) deleteDestination() error {
  if _, err := d.client.Delete(d.key, true); err != nil {
    return err
  }
  return nil
}
