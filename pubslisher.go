package isconn

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Publisher struct {
	cfg    *Config
	nc     *nats.Conn
	js     nats.JetStreamContext
	out    chan *nats.Msg
	outJS  chan *nats.Msg
	logger *zap.SugaredLogger
}

func NewPublisher(cfg *Config, nc *nats.Conn, js nats.JetStreamContext, out, outJS chan *nats.Msg, logger *zap.SugaredLogger) *Publisher {
	return &Publisher{
		cfg:    cfg,
		nc:     nc,
		js:     js,
		out:    out,
		outJS:  outJS,
		logger: logger,
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	defer func() {
		p.logger.Info("publisher stopped...")
	}()

	g := errgroup.Group{}

	// * NATS Core publish message
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-p.out:
				err := p.nc.PublishMsg(msg)
				if err != nil {
					p.logger.Errorf("nats publish: %w", err)
					p.out <- msg
				}
			}
		}
	})

	// * NATS Jeststream publish message
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-p.outJS:
				_, err := p.js.PublishMsg(msg, nats.RetryAttempts(1), nats.RetryWait(p.cfg.PublishRetryWait))
				if err != nil {
					p.logger.Errorf("nats js publish: %w", err)
					p.outJS <- msg
				}
			}
		}
	})

	return g.Wait()
}

func (p *Publisher) JsPublish(msg *nats.Msg) {
	p.outJS <- msg
}

func (p *Publisher) Publish(msg *nats.Msg) {
	p.out <- msg
}

func (p *Publisher) Request(msg *nats.Msg, timeout time.Duration) (*nats.Msg, error) {
	resp, err := p.nc.RequestMsg(msg, timeout)
	return resp, err
}
