package isconn

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	NatsURL                string
	ServiceName            string
	ServiceVersion         string
	ServiceSubjectPrefix   string
	ConnectTimeout         time.Duration
	ReconnectWait          time.Duration
	TotalWait              time.Duration
	ReconnectBufSize       int
	PublishRetryWait       time.Duration
	MaxWaitResp            time.Duration
	MaxWaitJsPull          time.Duration
	RatryFailedMsgChanSize int
}

func DefaultConfig(name string) *Config {
	return &Config{
		NatsURL:                "nats://localhost:4222",
		ServiceName:            name,
		ServiceVersion:         "0.0.1",
		ServiceSubjectPrefix:   name,
		ConnectTimeout:         time.Second * 10,
		ReconnectWait:          time.Second,
		TotalWait:              time.Second * 300,
		ReconnectBufSize:       100 * 1024 * 1024,
		PublishRetryWait:       time.Millisecond * 50,
		MaxWaitResp:            time.Second,
		MaxWaitJsPull:          time.Second,
		RatryFailedMsgChanSize: 1000,
	}
}

type Manager struct {
	NC     *nats.Conn
	JS     nats.JetStreamContext
	Pub    *Publisher
	Sub    *Subscriber
	Svc    *Service
	logger Logger
}

func (m *Manager) Run(ctx context.Context) error {
	g := errgroup.Group{}

	g.Go(func() error {
		return m.Sub.Run(ctx)
	})

	g.Go(func() error {
		return m.Pub.Run(ctx)
	})

	return g.Wait()
}

func (m *Manager) Stop() {
	if err := m.Svc.Stop(); err != nil {
		m.logger.Error(err)
	}
	if err := m.NC.Flush(); err != nil {
		m.logger.Error(err)
	}
	m.NC.Close()
	m.logger.Info("isconn manager stopped")
}

func NewManager(cfg *Config, logger Logger) (*Manager, error) {
	nc, err := NewNatsClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	js, err := NewNatsJS(cfg, nc)
	if err != nil {
		return nil, err
	}
	svc, err := NewService(nc, cfg.ServiceName, cfg.ServiceVersion, cfg.ServiceSubjectPrefix)
	if err != nil {
		return nil, err
	}
	return &Manager{
		NC:     nc,
		JS:     js,
		Pub:    NewPublisher(cfg, nc, js, logger),
		Sub:    NewSubscriber(cfg, nc, js, logger),
		Svc:    svc,
		logger: logger,
	}, nil
}
