package bootstrap

import (
	"errors"
	"net"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config contains explicit local worker wiring. Never log this value: it includes credentials.
// Credentials are supplied explicitly through environment configuration.
type Config struct {
	AWSRegion, AWSAccessKeyID, AWSSecretAccessKey, AWSSessionToken string
	DatabaseURL                                                    string
	SQSEndpoint                                                    string
	InputQueueURL                                                  string
	OutputQueueURL                                                 string
	ProviderID                                                     string
	ConsumerName                                                   string
	HTTPAddress                                                    string
	OIDCIssuer                                                     string
	OIDCAudience                                                   string
}

// LoadConfig reads process environment only; it does not load or overwrite .env files.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{DatabaseURL: getenv("WALLET_DATABASE_URL"), SQSEndpoint: getenv("WALLET_SQS_ENDPOINT"), InputQueueURL: getenv("WALLET_INPUT_QUEUE_URL"), OutputQueueURL: getenv("WALLET_OUTPUT_QUEUE_URL"), ProviderID: getenv("WALLET_PROVIDER_ID"), ConsumerName: getenv("WALLET_CONSUMER_NAME"), HTTPAddress: getenv("WALLET_HTTP_ADDRESS"), OIDCIssuer: getenv("WALLET_OIDC_ISSUER"), OIDCAudience: getenv("WALLET_OIDC_AUDIENCE"), AWSRegion: getenv("AWS_REGION"), AWSAccessKeyID: getenv("AWS_ACCESS_KEY_ID"), AWSSecretAccessKey: getenv("AWS_SECRET_ACCESS_KEY"), AWSSessionToken: getenv("AWS_SESSION_TOKEN")}
	return c, c.Validate()
}

// Validate rejects incomplete configuration without echoing connection strings or credentials.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return errors.New("WALLET_DATABASE_URL is required")
	}
	if _, e := pgxpool.ParseConfig(c.DatabaseURL); e != nil {
		return errors.New("invalid WALLET_DATABASE_URL")
	}
	endpoint, e := url.Parse(c.SQSEndpoint)
	if e != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.Host == "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return errors.New("invalid local SQS endpoint")
	}
	if c.AWSRegion == "" || c.AWSAccessKeyID == "" || c.AWSSecretAccessKey == "" {
		return errors.New("AWS_REGION and explicit AWS credentials are required")
	}
	for _, raw := range []string{c.InputQueueURL, c.OutputQueueURL} {
		q, e := url.Parse(raw)
		if e != nil || q.Scheme != endpoint.Scheme || q.Host != endpoint.Host || q.User != nil || q.RawQuery != "" || q.Fragment != "" || strings.Trim(q.Path, "/") == "" {
			return errors.New("queue URLs must use the configured local endpoint")
		}
	}
	if !strings.HasSuffix(c.InputQueueURL, ".fifo") || strings.HasSuffix(c.OutputQueueURL, ".fifo") {
		return errors.New("input must be FIFO and output must be standard")
	}
	if c.ProviderID == "" || strings.TrimSpace(c.ProviderID) != c.ProviderID || c.ConsumerName == "" || strings.TrimSpace(c.ConsumerName) != c.ConsumerName {
		return errors.New("provider and stable consumer name are required")
	}
	if c.HTTPAddress != "" || c.OIDCIssuer != "" || c.OIDCAudience != "" {
		host, port, e := net.SplitHostPort(c.HTTPAddress)
		ip := net.ParseIP(host)
		if e != nil || ip == nil || (!ip.IsLoopback() && !ip.IsUnspecified()) || port == "" {
			return errors.New("HTTP requires an explicit loopback or wildcard listen address")
		}
		issuer, e := url.Parse(c.OIDCIssuer)
		if e != nil || issuer.Host == "" || (issuer.Scheme != "http" && issuer.Scheme != "https") || issuer.User != nil || issuer.RawQuery != "" || issuer.Fragment != "" || strings.TrimSpace(c.OIDCAudience) == "" {
			return errors.New("HTTP requires OIDC issuer and audience")
		}
	}
	return nil
}
