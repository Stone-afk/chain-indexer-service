package listener_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/listener"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

type subscription struct {
	errs    chan error
	stopped chan struct{}
}

func (s *subscription) Err() <-chan error { return s.errs }
func (s *subscription) Unsubscribe()      { close(s.stopped) }

type subscriber func(context.Context, chan<- domain.BlockHeader) (provider.Subscription, error)

func (s subscriber) SubscribeNewHeads(c context.Context, h chan<- domain.BlockHeader) (provider.Subscription, error) {
	return s(c, h)
}

type handler func(context.Context) error

func (h handler) HandleNewHead(c context.Context) error { return h(c) }

func TestListenerSerialHeadsAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &subscription{make(chan error, 1), make(chan struct{})}
	var active, calls atomic.Int32
	entered, release := make(chan struct{}, 2), make(chan struct{})
	sub := subscriber(func(c context.Context, heads chan<- domain.BlockHeader) (provider.Subscription, error) {
		go func() {
			for _, n := range []int64{7, 8} {
				select {
				case heads <- domain.BlockHeader{Number: n}:
				case <-c.Done():
					return
				}
			}
		}()
		return s, nil
	})
	h := handler(func(context.Context) error {
		if active.Add(1) != 1 {
			t.Error("concurrent head handler")
		}
		defer active.Add(-1)
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- listener.New(sub, h).Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first head not handled")
	}
	select {
	case <-entered:
		t.Fatal("second handler overtook first")
	default:
	}
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("second head not handled")
	}
	release <- struct{}{}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop listener")
	}
	select {
	case <-s.stopped:
	default:
		t.Fatal("subscription not unsubscribed")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestListenerReturnsFailures(t *testing.T) {
	want := errors.New("sentinel")
	for _, mode := range []string{"subscribe", "subscription", "handler"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			s := &subscription{make(chan error, 1), make(chan struct{})}
			sub := subscriber(func(c context.Context, heads chan<- domain.BlockHeader) (provider.Subscription, error) {
				if mode == "subscribe" {
					return nil, want
				}
				if mode == "subscription" {
					s.errs <- want
				} else {
					go func() {
						select {
						case heads <- domain.BlockHeader{Number: 1}:
						case <-c.Done():
						}
					}()
				}
				return s, nil
			})
			err := listener.New(sub, handler(func(context.Context) error { return want })).Run(ctx)
			if !errors.Is(err, want) {
				t.Fatalf("got %v, want sentinel", err)
			}
			if mode != "subscribe" {
				select {
				case <-s.stopped:
				default:
					t.Fatal("failed listener leaked subscription")
				}
			}
		})
	}
}
