package pipes

import (
  "github.com/coreos/go-etcd/etcd"
  "sync"
  "time"
  "strings"
)

type destination struct {
  // etcd is the distributed store for service discovery
  client *etcd.Client

  // The name of the destination
  name string

  // The etcd key of the destination
  key string

  // Channel to stop registration goroutine
  regDone chan bool

  // Channel to send errors about destination registration
  regErrChan chan error

  // Channel to send responses from group watch
  watchGroupRespChan chan *etcd.Response

  // Channel to send errors about group watch
  watchGroupErrChan chan error

  // Channel to stop watch group goroutine
  watchGroupDone chan bool

  // Channel to stop handle group changes goroutine
  handleGroupChangesDone chan bool

  // Channel to send errors about group change handling
  handleGroupChangesErrChan chan error

  // Used to wait for goroutines to finish
  wg sync.WaitGroup

  // Create index of destination in Etcd
  createdIndex uint64

  // Pipes associated with this destination
  pipes map[string]*pipe
}

func NewDestination(client *etcd.Client, name string) *destination {
    d := &destination{
      client: client,
      name: name,
      key: "/streams/destinations/" + name,
      regDone: make(chan bool),
      regErrChan: make(chan error),
      watchGroupDone: make(chan bool),
      watchGroupErrChan: make(chan error),
      handleGroupChangesDone: make(chan bool),
      handleGroupChangesErrChan: make(chan error),
      createdIndex: 0,
      pipes: make(map[string]*pipe),
    }
    return d
}

func (d *destination) Register(reregister bool) {
  // TODO: If the call to createDestination returns an error, it will block
  // until the calling program reads from the registration error channel. If the
  // calling program hasn't yet started reading from the error channel, this will
  // block forever.

  // Create destination in etcd
  if err := d.createDestination(); err != nil {
    d.regErrChan <- err
  }

  // Launch goroutine for updating the destination TTL value
  d.wg.Add(1)
  go d.maintainDestination(reregister)

  // Launch goroutine to watch destination directory for group changes
  d.wg.Add(1)
  go d.watchForGroups()

  // Launch goroutine to handle events from the group watch goroutine
  //  - When a new group is created, create and open a new pipe
  //  - When a group is deleted, close and delete the group's pipe
  d.wg.Add(1)
  go d.handleGroupChanges()
}

func (d *destination) Unregister() {
  d.watchGroupDone <- true
  d.handleGroupChangesDone <- true
  d.regDone <- true
  d.wg.Wait()
}

func (d destination) RegistrationErrorChannel() <-chan error {
  return d.regErrChan
}

func (d destination) WatchGroupErrorChannel() <-chan error {
  return d.watchGroupErrChan
}

func (d destination) HandleGroupChangesErrorChannel() <-chan error {
  return d.handleGroupChangesErrChan
}

func (d *destination) maintainDestination(reregister bool) {
  defer d.wg.Done()
  for {
    select {
    case <- d.regDone:
        // when a signal arrives on this channel, the Unregister method has
        // been called, so we'll delete the destination and exit this
        // goroutine
        if err := d.deleteDestination(); err != nil {
          d.regErrChan <- err
        }
        return
    default:
      time.Sleep(1* time.Second)
      if err := d.refreshDestinationTtl(2); err != nil {
        e := err.(*etcd.EtcdError)
        // If we lost connection to etcd, send the error, but keep attempting
        // future updates
        if e.ErrorCode == 501 {
            d.regErrChan <- err

        // Else, if the destination directory is not present, we'll either:
        //   - if automatically re-registerting, attempt to re-register
        //   - if not automatically re-registering, send the error and return
        } else if e.ErrorCode == 100 {
          if reregister == true {
            if err := d.createDestination(); err != nil {
              d.regErrChan <- err
            }
          } else {
            d.regErrChan <- err
            return
          }
        // Else, this is an unexpected error, so we'll let the caller know.
        } else {
          d.regErrChan <- err
        }
      }
    }
  }
}

func (d *destination) watchForGroups() {
  defer d.wg.Done()
  d.watchGroupRespChan = make(chan *etcd.Response)
  if _, err := d.client.Watch(d.key, d.createdIndex + 1, true, d.watchGroupRespChan, d.watchGroupDone); err != nil {
    if err != etcd.ErrWatchStoppedByUser {
      d.watchGroupErrChan <- err
    }
  }
}

func (d *destination) handleGroupChanges() {
  defer d.wg.Done()
  for {
    select {
    case resp := <-d.watchGroupRespChan:
      if resp.Action == "create" {
        if len(strings.Split(resp.Node.Key, "/")) == 5 {
          pipe := NewPipe(d.client, resp.Node.Key)
          if err := pipe.Open(); err != nil {
            d.handleGroupChangesErrChan <- err
          }
          d.pipes[resp.Node.Key] = pipe
        }
      } else if resp.Action == "expire" {
        if pipe, ok := d.pipes[resp.Node.Key]; ok {
          pipe.Close()
          delete(d.pipes, key)
        }
      }
    case <- d.handleGroupChangesDone:
      for key, pipe := range d.pipes {
        pipe.Close()
        delete(d.pipes, key)
      }
      return
    }
  }
}

func (d destination) refreshDestinationTtl(ttl uint64) error {
  if _, err := d.client.UpdateDir(d.key, ttl); err != nil {
    return err
  }
  return nil
}

func (d *destination) createDestination() error {
  if resp, err := d.client.CreateDir(d.key, 2); err != nil {
    // In contrast to the group, we should return an error if the destination
    // already exists, because only one process can send to a destination.
    // TODO: Do we want to allow multiple destination senders?  Do pipelines
    // work with both multiple senders and multiple receivers?
    return err
  } else {
    d.createdIndex = resp.Node.CreatedIndex
  }
  return nil
}

func (d destination) deleteDestination() error {
  if _, err := d.client.Delete(d.key, true); err != nil {
    return err
  }
  return nil
}
