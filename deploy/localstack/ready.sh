#!/bin/sh
set -eu
# All credentials and account identifiers here belong only to the local emulator.
awslocal sqs create-queue --queue-name jungle-wallet-events --attributes VisibilityTimeout=30,MessageRetentionPeriod=345600,ReceiveMessageWaitTimeSeconds=10 >/dev/null
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true,ContentBasedDeduplication=false,MessageRetentionPeriod=1209600 >/dev/null
# JSON file avoids platform-dependent nested shell escaping of redrive attributes.
cat >/tmp/wager-queue-attributes.json <<'JSON'
{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"60","ReceiveMessageWaitTimeSeconds":"10","MessageRetentionPeriod":"345600","RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:wager-transactions-dlq.fifo\",\"maxReceiveCount\":\"5\"}"}
JSON
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes file:///tmp/wager-queue-attributes.json >/dev/null
