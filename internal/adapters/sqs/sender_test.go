package sqs

import (
	"context"
	"errors"
	"testing"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// TestSenderConfiguration rejects destinations incompatible with the standard queue contract.
func TestSenderConfiguration(t *testing.T) {
	client := awssqs.New(awssqs.Options{Region: "us-east-1"})
	for _, destination := range []string{"", "://bad", "ftp://localhost/queue", "http://localhost", "http://user:secret@localhost/queue", "http://localhost/queue.fifo", "http://localhost/queue?token=secret", "http://localhost/queue#fragment"} {
		if _, e := NewSender(client, destination); e == nil {
			t.Errorf("accepted %q", destination)
		}
	}
	if _, e := NewSender(nil, "http://localhost/queue"); e == nil {
		t.Fatal("accepted nil client")
	}
	s, e := NewSender(client, "http://localhost/queue")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = s.Send(ctx, []byte(`{}`)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e = s.Send(context.Background(), nil); e == nil {
		t.Fatal("accepted empty event")
	}
}
