package isconn

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type Config struct {
	NatsURL                string
	ConnectTimeout         time.Duration
	ReconnectWait          time.Duration
	TotalWait              time.Duration
	ReconnectBufSize       int
	PublishAsyncMaxPending int
	PublishRetryWait       time.Duration
	RetryAttempts          int
	MaxWaitResp            time.Duration
	MaxWaitJsPull          time.Duration
	RatryFailedMsgChanSize int
}

func DefaultNatsErrHandler(logger Logger) nats.ErrHandler {
	return func(c *nats.Conn, sub *nats.Subscription, err error) {
		switch sub {
		case nil:
			logger.Errorf("nats error handler: no subject: %w", err)
		default:
			logger.Errorf("nats error handler: subject = %s: %w", sub.Subject, err)
		}
	}
}

func DefaultNatsDisconnectErrHandler(logger Logger) nats.ConnErrHandler {
	return func(c *nats.Conn, err error) {
		logger.Errorf("nats disconected: %v", err)
	}
}

func DefaultNatsReconnectHandler(logger Logger) nats.ConnHandler {
	return func(c *nats.Conn) {
		logger.Infof("nats reconnected: %s ....", c.ConnectedUrl())
	}
}

func DefaultConfig() *Config {
	return &Config{
		NatsURL:                "nats://localhost:4222",
		ConnectTimeout:         time.Second * 10,
		ReconnectWait:          time.Second,
		TotalWait:              time.Second * 300,
		ReconnectBufSize:       100 * 1024 * 1024,
		PublishAsyncMaxPending: 256,
		PublishRetryWait:       time.Millisecond * 50,
		RetryAttempts:          3,
		MaxWaitResp:            time.Second,
		MaxWaitJsPull:          time.Second,
		RatryFailedMsgChanSize: 1000,
	}
}

func NewNatsClient(cfg *Config, logger Logger) (*nats.Conn, error) {
	nc, err := nats.Connect(
		cfg.NatsURL,
		nats.Timeout(cfg.ConnectTimeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(int(cfg.TotalWait/cfg.ReconnectWait)),
		nats.ReconnectBufSize(cfg.ReconnectBufSize),
		nats.ErrorHandler(DefaultNatsErrHandler(logger)),
		nats.DisconnectErrHandler(DefaultNatsDisconnectErrHandler(logger)),
		nats.ReconnectHandler(DefaultNatsReconnectHandler(logger)),
	)
	if err != nil {
		return nil, fmt.Errorf("create nats client: %v", err)
	}
	return nc, nil
}

func NewNatsJS(cfg *Config, nc *nats.Conn) (nats.JetStreamContext, error) {
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(cfg.PublishAsyncMaxPending), nats.MaxWait(cfg.MaxWaitResp))
	if err != nil {
		return nil, fmt.Errorf("create js context: %v", err)
	}
	return js, nil
}
