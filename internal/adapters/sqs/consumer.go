package sqs

import (
	"context"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
)

// MessageHandler returns nil only after durable handling; it must honor cancellation.
type MessageHandler interface {
	HandleMessage(context.Context, string, string, string, []byte) (application.WagerResult, error)
}

// ConsumerPolicy bounds one message's processing; no visibility heartbeat is required.
// ProviderID is a trusted queue-to-provider binding, never read from the payload.
type ConsumerPolicy struct {
	QueueURL, ConsumerName, ProviderID                                   string
	WaitSeconds, VisibilitySeconds, InitialRetrySeconds, MaxRetrySeconds int32
	ProcessingTimeout                                                    time.Duration
}

// Consumer handles one message per receive, avoiding invisible batches waiting locally.
type Consumer struct {
	client  *awssqs.Client
	handler MessageHandler
	policy  ConsumerPolicy
}

// DeliveryResult distinguishes no work, durable completion and a scheduled retry.
type DeliveryResult struct{ Received, Completed, Retried bool }

// NewConsumer validates FIFO routing and reserves ten seconds for acknowledgement overhead.
func NewConsumer(client *awssqs.Client, handler MessageHandler, p ConsumerPolicy) (*Consumer, error) {
	u, e := url.Parse(p.QueueURL)
	if client == nil || handler == nil || e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasSuffix(u.Path, ".fifo") ||
		p.ConsumerName == "" || strings.TrimSpace(p.ConsumerName) != p.ConsumerName || p.ProviderID == "" || strings.TrimSpace(p.ProviderID) != p.ProviderID ||
		p.WaitSeconds < 1 || p.WaitSeconds > 20 || p.VisibilitySeconds < 11 || p.VisibilitySeconds > 43200 || p.ProcessingTimeout <= 0 || p.ProcessingTimeout > time.Duration(p.VisibilitySeconds-10)*time.Second || p.InitialRetrySeconds < 1 || p.MaxRetrySeconds < p.InitialRetrySeconds || p.MaxRetrySeconds > 43200-p.VisibilitySeconds {
		return nil, errors.New("invalid SQS consumer configuration")
	}
	return &Consumer{client, handler, p}, nil
}

// retryDelay doubles with saturation; the queue's redrive policy limits total receives.
func (c *Consumer) retryDelay(attempt int) int32 {
	d := c.policy.InitialRetrySeconds
	for i := 1; i < attempt && d < c.policy.MaxRetrySeconds; i++ {
		if d > c.policy.MaxRetrySeconds/2 {
			return c.policy.MaxRetrySeconds
		}
		d *= 2
	}
	if d > c.policy.MaxRetrySeconds {
		return c.policy.MaxRetrySeconds
	}
	return d
}

// release relinquishes work on shutdown, using a fresh bounded cleanup context.
func (c *Consumer) release(receipt *string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, e := c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.policy.QueueURL), ReceiptHandle: receipt, VisibilityTimeout: 0})
	if e != nil {
		return errors.New("SQS release failed; visibility expiry will allow redelivery")
	}
	return nil
}

// RunOnce deletes only after handler success. Errors and malformed messages remain for redrive.
// Business rejections are successful durable handling; transport failures stop the worker.
func (c *Consumer) RunOnce(ctx context.Context) (DeliveryResult, error) {
	var result DeliveryResult
	if e := ctx.Err(); e != nil {
		return result, e
	}
	receiveCtx, cancel := context.WithTimeout(ctx, time.Duration(c.policy.WaitSeconds+5)*time.Second)
	out, e := c.client.ReceiveMessage(receiveCtx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(c.policy.QueueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: c.policy.WaitSeconds, VisibilityTimeout: c.policy.VisibilitySeconds, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount}})
	cancel()
	if e != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, errors.New("SQS receive failed")
	}
	if len(out.Messages) == 0 {
		return result, nil
	}
	m := out.Messages[0]
	result.Received = true
	if aws.ToString(m.ReceiptHandle) == "" {
		return result, errors.New("SQS receipt missing")
	}
	if ctx.Err() != nil {
		return result, errors.Join(ctx.Err(), c.release(m.ReceiptHandle))
	}
	attempt, e := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
	if e != nil || attempt < 1 {
		return result, errors.New("SQS receive count missing")
	}
	processingCtx, done := context.WithTimeout(ctx, c.policy.ProcessingTimeout)
	// SQS MessageId is a correlation fallback only, not the inbox identity or authority.
	_, e = c.handler.HandleMessage(processingCtx, c.policy.ConsumerName, c.policy.ProviderID, aws.ToString(m.MessageId), []byte(aws.ToString(m.Body)))
	if e == nil {
		e = processingCtx.Err()
	}
	done()
	if ctx.Err() != nil {
		return result, errors.Join(ctx.Err(), c.release(m.ReceiptHandle))
	}
	actionCtx, finish := context.WithTimeout(ctx, 5*time.Second)
	defer finish()
	if e != nil {
		_, e = c.client.ChangeMessageVisibility(actionCtx, &awssqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.policy.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: c.retryDelay(attempt)})
		if e != nil {
			if ctx.Err() != nil {
				return result, errors.Join(ctx.Err(), c.release(m.ReceiptHandle))
			}
			return result, errors.New("SQS retry scheduling failed")
		}
		telemetry.From(ctx).Count("wallet_sqs_retries_total")
		result.Retried = true
		return result, nil
	}
	_, e = c.client.DeleteMessage(actionCtx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(c.policy.QueueURL), ReceiptHandle: m.ReceiptHandle})
	if e != nil {
		if ctx.Err() != nil {
			return result, errors.Join(ctx.Err(), c.release(m.ReceiptHandle))
		}
		return result, errors.New("SQS acknowledgement failed")
	}
	result.Completed = true
	return result, nil
}

// Run polls sequentially until cancellation or a transport failure requiring supervision.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if _, e := c.RunOnce(ctx); e != nil {
			return e
		}
	}
}
