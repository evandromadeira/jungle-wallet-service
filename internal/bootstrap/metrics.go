package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"strconv"
	"strings"
)

// collectMetrics reads durable backlog and the configured DLQ without consuming messages.
func (r *runtime) collectMetrics(ctx context.Context, client *awssqs.Client) (map[string]float64, error) {
	values := map[string]float64{}
	var pending, lag float64
	if err := r.pool.QueryRow(ctx, "SELECT count(*),COALESCE(GREATEST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP-min(created_at)),0),0)::float8 FROM public.outbox_events WHERE published_at IS NULL").Scan(&pending, &lag); err != nil {
		return nil, err
	}
	values["wallet_outbox_pending"] = pending
	values["wallet_outbox_oldest_age_seconds"] = lag
	input, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(r.config.InputQueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy}})
	if err != nil {
		return nil, err
	}
	var policy map[string]string
	if json.Unmarshal([]byte(input.Attributes["RedrivePolicy"]), &policy) != nil {
		return nil, errors.New("invalid redrive")
	}
	arn := strings.Split(policy["deadLetterTargetArn"], ":")
	if len(arn) != 6 || arn[5] == "" {
		return nil, errors.New("invalid DLQ ARN")
	}
	queue, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(arn[5]), QueueOwnerAWSAccountId: aws.String(arn[4])})
	if err != nil {
		return nil, err
	}
	stats, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		return nil, err
	}
	for attr, name := range map[string]string{"ApproximateNumberOfMessages": "wallet_dlq_visible_messages", "ApproximateNumberOfMessagesNotVisible": "wallet_dlq_inflight_messages"} {
		value, e := strconv.ParseUint(stats.Attributes[attr], 10, 64)
		if e != nil {
			return nil, e
		}
		values[name] = float64(value)
	}
	return values, nil
}
