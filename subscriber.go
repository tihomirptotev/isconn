package isconn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

type Subscriber struct {
	cfg    *Config
	g      errgroup.Group
	nc     *nats.Conn
	js     nats.JetStreamContext
	tasks  []func(context.Context) error
	mu     sync.Mutex
	logger Logger
}

func NewSubscriber(cfg *Config, nc *nats.Conn, js nats.JetStreamContext, logger Logger) *Subscriber {
	return &Subscriber{
		cfg:    cfg,
		nc:     nc,
		js:     js,
		tasks:  make([]func(context.Context) error, 0),
		logger: logger,
	}
}

func (s *Subscriber) Run(ctx context.Context) error {
	defer s.logger.Info("subscriber stopped...")
	g := errgroup.Group{}

	s.mu.Lock()
	for _, task := range s.tasks {
		t := task
		g.Go(func() error {
			for {
				err := t(ctx)
				if err == nil {
					return nil
				}
				s.logger.Errorf("subscribe: %v: trying to reconnect", err)
			}
		})
	}
	s.mu.Unlock()

	return s.g.Wait()
}

func (s *Subscriber) AddJsPullSubscription(ctx context.Context, subject, durable string, batch int, handler func(msgs []*nats.Msg), opts ...nats.SubOpt) {

	task := func(ctx context.Context) error {
		sub, err := s.js.PullSubscribe(subject, durable, opts...)
		if err != nil {
			return fmt.Errorf("nats: pull subscribe to %s: %v", subject, err)
		}
		defer sub.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				msgs, err := sub.Fetch(batch, nats.MaxWait(s.cfg.MaxWaitJsPull))
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				if err != nil {
					s.logger.Errorf("nats: fetch: %v", err)
					return err
				}
				handler(msgs)
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)

}

func (s *Subscriber) AddSyncSubscription(ctx context.Context, subject string, msgLimit, bytesLimit int, f func(m *nats.Msg)) {

	task := func(ctx context.Context) error {
		sub, err := s.nc.SubscribeSync(subject)
		if err != nil {
			return fmt.Errorf("nats sync subscribe to %s: %v", subject, err)
		}
		defer sub.Drain()
		if err := sub.SetPendingLimits(msgLimit, bytesLimit); err != nil {
			return fmt.Errorf("nats: set max pending limits for %s: %v", subject, err)
		}
		s.logger.Infof("nats subscribe to channel %s, starting to process incommig messages...", subject)

		for {
			msg, err := sub.NextMsgWithContext(ctx)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if err != nil {
				return err
			}
			f(msg)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)

}

func (s *Subscriber) AddChanSyncSubscription(ctx context.Context, subject string, chanSize, msgLimit, bytesLimit int, f func(m *nats.Msg)) {

	task := func(ctx context.Context) error {
		sub, err := s.nc.SubscribeSync(subject)
		if err != nil {
			return fmt.Errorf("nats sync subscribe to %s: %v", subject, err)
		}
		defer sub.Drain()
		if err := sub.SetPendingLimits(msgLimit, bytesLimit); err != nil {
			return fmt.Errorf("nats: set max pending limits for %s: %v", subject, err)
		}
		s.logger.Infof("nats subscribe to channel %s, starting to process incommig messages...", subject)

		msgCh := make(chan *nats.Msg, chanSize)
		defer close(msgCh)

		s.g.Go(func() error {
			for msg := range msgCh {
				f(msg)
			}
			return nil
		})

		for {
			msg, err := sub.NextMsgWithContext(ctx)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if err != nil {
				return err
			}
			msgCh <- msg
		}

	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)

}
