package isconn

import (
	"context"
	"testing"
	"time"

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

	g.Go(func() error {
		return manager.Run(ctx)
	})

	g.Go(func() error {
		time.Sleep(time.Second * 3)
		cancel()
		return nil
	})

	require.NoError(t, g.Wait())

	manager.Stop()

}
