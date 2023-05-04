package isconn

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func TestManager(t *testing.T) {
	cfg := DefaultConfig("test-service")
	l, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer l.Sync()
	logger := l.Sugar()
	manager, err := NewManager(cfg, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	g := errgroup.Group{}

	err = manager.Svc.AddEndpoint("proba", func(r micro.Request) {
		r.Respond([]byte("ok"))
	})
	require.NoError(t, err)

	g.Go(func() error {
		return manager.Run(ctx)
	})

	g.Go(func() error {
		time.Sleep(time.Second * 2)
		cancel()
		return nil
	})

	resp, err := manager.Pub.Request(&nats.Msg{
		Subject: "test-service.proba",
		Data:    nil,
	}, time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), resp.Data)

	require.NoError(t, g.Wait())

	manager.Stop()

}
