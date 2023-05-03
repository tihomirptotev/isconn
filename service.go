package isconn

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type NatsService struct {
	svc   micro.Service
	group micro.Group
}

func (s *NatsService) AddEndpoint(subject string, f micro.HandlerFunc) {
	s.group.AddEndpoint(subject, micro.HandlerFunc(f))
}

func (s *NatsService) Stop() error {
	return s.svc.Stop()
}

func NewNatsService(nc *nats.Conn, name, version, groupPrefix string) (*NatsService, error) {
	svc, err := micro.AddService(nc, micro.Config{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return nil, err
	}

	return &NatsService{
		svc:   svc,
		group: svc.AddGroup(groupPrefix),
	}, nil
}
