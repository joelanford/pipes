package pipes

import (
  "github.com/coreos/go-etcd/etcd"
  "github.com/gdamore/mangos"
  "github.com/gdamore/mangos/protocol/push"
  "github.com/gdamore/mangos/transport/tcp"
)

type pipe struct {
  // etcd is the distributed store for service discovery
  client *etcd.Client

  // The URL key in etcd
  key string

  // The url of the Socket
  url string

  // The socket
  socket mangos.Socket
}

func NewPipe(client *etcd.Client, groupKey string, url string) *pipe {
    p := &pipe{
      client: client,
      key: groupKey + "/url",
      url: url,
    }
    return p
}

func (p *pipe) Open() error {
  if err := p.createSocket(); err != nil {
    return err
  } else if err := p.createUrl(); err != nil {
    return err
  }
  return nil
}

func (p *pipe) Close() error {
  if err := p.deleteUrl(); err != nil {
    return err
  }
  p.socket.Close()
  return nil
}

func (p pipe) Send(msg []byte) error {
  if err := p.socket.Send(msg); err != nil {
    return err
  }
  return nil
}

func (p *pipe) createUrl() error {
  if _, err := p.client.Create(p.key, p.url, 0); err != nil {
    return err
  }
  return nil
}

func (p *pipe) deleteUrl() error {
  if _, err := p.client.Delete(p.key, true); err != nil {
    return err
  }
  return nil
}

func (p *pipe) createSocket() error {
  if sock, err := push.NewSocket(); err != nil {
    return err
  } else {
    sock.AddTransport(tcp.NewTransport())
    if err = sock.Listen(p.url); err != nil {
      return err
    } else {
      p.socket = sock
    }
  }
  return nil
}
