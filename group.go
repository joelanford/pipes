package pipes

import (
  "github.com/coreos/go-etcd/etcd"
  "sync"
  "time"
)

type group struct {
  // etcd is the distributed store for service discovery
  client *etcd.Client

  // The name of the group
  name string

  // The etcd key of the destination
  destinationKey string

  // The etcd key of the group
  key string

  // Channel to signal goroutines to end
  joinDone chan bool

  // Channel to send errors
  joinErrChan chan error

  // Create index of group in Etcd
  createdIndex uint64

  watchUrlDone chan bool
  watchUrlRespChan chan *etcd.Response
  watchUrlErrChan chan error

  handleUrlChangesDone chan bool
  handleUrlChangesErrChan chan error

  // Used to wait for goroutines to finish
  wg sync.WaitGroup
}

func NewGroup(client *etcd.Client, destination string, name string) *group {
    d := &group{
      client: client,
      name: name,
      destinationKey: "/streams/destinations/" + destination,
      key: "/streams/destinations/" + destination + "/" + name,
      joinDone: make(chan bool),
      joinErrChan: make(chan error),
      watchUrlDone: make(chan bool),
      watchUrlErrChan: make(chan error),
      handleUrlChangesDone: make(chan bool),
      handleUrlChangesErrChan: make(chan error),
      createdIndex: 0,
    }
    return d
}

func (g *group) Join(rejoin bool) {
  // TODO: If the calls to waitForDestination or createGroup return an error,
  // they will block until the calling program reads from the join error. If the
  // calling program hasn't yet started reading from the error channel, this
  // will block forever.

  // Wait for destination to be created
  if err := g.waitForDestination(); err != nil {
    g.joinErrChan <- err
  }

  // Create group in etcd
  if err := g.createGroup(2); err != nil {
    g.joinErrChan <- err
  }

  // Start goroutine to update the group ttl
  g.wg.Add(1)
  go g.maintainGroup(rejoin)

  // TODO: Start 2 goroutines to 1. watch for url changes
  //                             2. update the socket connection on changes
  //   - when url is created, connect to socket at url
  //   - when url changes, disconnect old socket and connect new socket
  //   - when url is deleted, disconnect socket
  g.wg.Add(1)
  go g.watchForUrls()

  g.wg.Add(1)
  go g.handleUrlChanges()
}

func (g *group) Leave() {
  g.joinDone <- true
  g.watchUrlDone <- true
  g.handleUrlChangesDone <- true
  g.wg.Wait()
}

func (g *group) JoinErrorChannel() <-chan error {
  return g.joinErrChan
}

func (g *group) WatchUrlErrorChannel() <-chan error {
  return g.watchUrlErrChan
}

func (g *group) maintainGroup(rejoin bool) {
  defer g.wg.Done()
  for {
    select {
    case <- g.joinDone:
      // when a signal arrives on this channel, the Leave method has
      // been called, so we'll end the goroutine. We don't want to delete the
      // group because there may be other members.  If we're the last member,
      // the group will expire on it's own shortly
      return
    default:
      time.Sleep(1* time.Second)
      if err := g.refreshGroupTtl(2); err != nil {
        e := err.(*etcd.EtcdError)
        // If we lost connection to etcd, send the error, but keep attempting
        // future updates
        if e.ErrorCode == 501 {
          g.joinErrChan <- err

        // Else, if the destination or group directory is not present, we'll
        // either:
        //   - if automatically re-rejoining, attempt to re-join
        //   - if not automatically re-rejoining, send the error and return
        } else if e.ErrorCode == 100 {
          if rejoin == true {
            if err := g.waitForDestination(); err != nil {
              g.joinErrChan <- err
            }
            if err := g.createGroup(2); err != nil {
              g.joinErrChan <- err
            }
          } else {
            g.joinErrChan <- err
            return
          }

        // Else, this is an unexpected error, so we'll let the caller know
        // and return immediately
        } else {
          g.joinErrChan <- err
          return
        }
      }
    }
  }
}

func (g group) refreshGroupTtl(ttl uint64) error {
  if _, err := g.client.UpdateDir(g.key, ttl); err != nil {
    return err
  }
  return nil
}

func (g group) waitForDestination() error {
  if _, err := g.client.Get(g.destinationKey, false, false); err != nil {
    e := err.(*etcd.EtcdError)
    // If the destination does not exist, we need to wait for it.
    if e.ErrorCode == 100 {
      // TODO: Pass a stop channel to this function and send true to it in group.Leave()
      if _, err := g.client.Watch(g.destinationKey, 0, false, nil, nil); err != nil {
        return err
      }

    // Otherwise, we got an unexpected error, so return it
    } else {
      return err
    }
  }
  return nil
}

func (g *group) createGroup(ttl uint64) error {
  if resp, err := g.client.CreateDir(g.key, ttl); err != nil {
    e := err.(*etcd.EtcdError)
    // If the group already exists (ErrorCode 105), no big deal. Otherwise, return
    // an error
    if e.ErrorCode == 105 {
      if resp, err := g.client.Get(g.destinationKey, false, false); err != nil {
        return err
      } else {
        g.createdIndex = resp.Node.CreatedIndex
      }
    } else {
      return err
    }
  } else {
    g.createdIndex = resp.Node.CreatedIndex
  }
  return nil
}

func (g *group) watchForUrls() {
  defer g.wg.Done()
  g.watchUrlRespChan = make(chan *etcd.Response)
  if _, err := g.client.Watch(g.key + "/url", g.createdIndex + 1, true, g.watchUrlRespChan, g.watchUrlDone); err != nil {
    if err != etcd.ErrWatchStoppedByUser {
      g.watchUrlErrChan <- err
    }
  }
}

func (g *group) handleUrlChanges() {
  defer g.wg.Done()
  for {
    select {
    case resp := <-g.watchUrlRespChan:
      if resp.Action == "create" {
        // TODO: Connect to socket at URL in resp.Node.Value
      } else if resp.Action == "expire" {
        // TODO: Disconnect socket
      }
    case <- g.handleUrlChangesDone:
      // TODO: Disconnect socket
      return
    }
  }
}
