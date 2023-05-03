package isconn

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type Service struct {
	svc   micro.Service
	group micro.Group
}

func (s *Service) AddEndpoint(subject string, f micro.HandlerFunc) {
	s.group.AddEndpoint(subject, micro.HandlerFunc(f))
}

func (s *Service) Stop() error {
	return s.svc.Stop()
}

func NewService(nc *nats.Conn, name, version, groupPrefix string) (*Service, error) {
	svc, err := micro.AddService(nc, micro.Config{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return nil, err
	}

	return &Service{
		svc:   svc,
		group: svc.AddGroup(groupPrefix),
	}, nil
}
