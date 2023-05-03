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

func NewPublisher(cfg *Config, nc *nats.Conn, js nats.JetStreamContext, logger *zap.SugaredLogger) *Publisher {
	return &Publisher{
		cfg:    cfg,
		nc:     nc,
		js:     js,
		out:    make(chan *nats.Msg, cfg.RatryFailedMsgChanSize),
		outJS:  make(chan *nats.Msg, cfg.RatryFailedMsgChanSize),
		logger: logger,
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	defer func() {
		close(p.out)
		close(p.outJS)
		p.logger.Info("publisher stopped...")
	}()

	g := errgroup.Group{}

	// * NATS Core publish message
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				t := time.NewTimer(time.Second * 3)
				defer t.Stop()
				for {
					select {
					case <-t.C:
						return nil
					case msg, ok := <-p.out:
						if !ok {
							return nil
						}
						p.Publish(msg)
					}
				}
			case msg := <-p.out:
				p.Publish(msg)
			}
		}
	})

	// * NATS Jeststream publish message
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				t := time.NewTimer(time.Second * 3)
				defer t.Stop()
				for {
					select {
					case <-t.C:
						return nil
					case msg, ok := <-p.outJS:
						if !ok {
							return nil
						}
						p.JsPublish(msg)
					}
				}
			case msg := <-p.outJS:
				p.JsPublish(msg)
			}
		}
	})

	return g.Wait()
}

func (p *Publisher) JsPublish(msg *nats.Msg) {
	_, err := p.js.PublishMsg(msg, nats.RetryAttempts(1), nats.RetryWait(p.cfg.PublishRetryWait))
	if err != nil {
		p.logger.Errorf("nats js publish: %w", err)
		defer func() {
			if r := recover(); r != nil {
				p.logger.Errorf("recovered in JsPublish: %v", r)
			}
		}()
		p.outJS <- msg
	}
}

func (p *Publisher) Publish(msg *nats.Msg) {
	err := p.nc.PublishMsg(msg)
	if err != nil {
		p.logger.Errorf("nats publish: %w", err)
		defer func() {
			if r := recover(); r != nil {
				p.logger.Errorf("recovered in Publish: %v", r)
			}
		}()
		p.out <- msg
	}
}

func (p *Publisher) Request(msg *nats.Msg, timeout time.Duration) (*nats.Msg, error) {
	resp, err := p.nc.RequestMsg(msg, timeout)
	return resp, err
}
