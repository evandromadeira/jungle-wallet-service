package sqs

import (
	"context"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
)

type unusedHandler struct{}

// HandleMessage panics if constructor or pre-cancelled work unexpectedly processes data.
func (unusedHandler) HandleMessage(context.Context, string, string, string, []byte) (application.WagerResult, error) {
	panic("unexpected processing")
}

// TestConsumerPolicy checks visibility budgets and exponential retry saturation.
func TestConsumerPolicy(t *testing.T) {
	client := awssqs.New(awssqs.Options{Region: "us-east-1"})
	valid := ConsumerPolicy{QueueURL: "http://localhost/wagers.fifo", ConsumerName: "wagers", ProviderID: "a", WaitSeconds: 10, VisibilitySeconds: 60, ProcessingTimeout: 30 * time.Second, InitialRetrySeconds: 2, MaxRetrySeconds: 9}
	c, e := NewConsumer(client, unusedHandler{}, valid)
	if e != nil {
		t.Fatal(e)
	}
	for i, want := range []int32{2, 4, 8, 9, 9} {
		if c.retryDelay(i+1) != want {
			t.Fatal("incorrect retry delay")
		}
	}
	if c.retryDelay(2147483647) != 9 {
		t.Fatal("retry did not saturate")
	}
	for _, mutate := range []func(*ConsumerPolicy){func(p *ConsumerPolicy) { p.ProcessingTimeout = 51 * time.Second }, func(p *ConsumerPolicy) { p.WaitSeconds = 21 }, func(p *ConsumerPolicy) { p.ProviderID = "" }, func(p *ConsumerPolicy) { p.QueueURL = "http://localhost/standard" }, func(p *ConsumerPolicy) { p.InitialRetrySeconds = 0 }, func(p *ConsumerPolicy) { p.MaxRetrySeconds = 43200 }} {
		p := valid
		mutate(&p)
		if _, e := NewConsumer(client, unusedHandler{}, p); e == nil {
			t.Fatal("accepted invalid policy")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := c.Run(ctx); e != context.Canceled {
		t.Fatal(e)
	}
}
