package bootstrap

import (
	"strings"
	"testing"
)

// testConfig supplies valid syntax without requiring any network resources.
func testConfig() Config {
	return Config{AWSRegion: "us-east-1", AWSAccessKeyID: "test", AWSSecretAccessKey: "test", DatabaseURL: "postgres://u:secret@localhost/db", SQSEndpoint: "http://127.0.0.1:4566", InputQueueURL: "http://127.0.0.1:4566/000000000000/in.fifo", OutputQueueURL: "http://127.0.0.1:4566/000000000000/out", ProviderID: "a", ConsumerName: "wagers-a"}
}

// TestComposition verifies the Fx graph without opening sockets or pools.
func TestComposition(t *testing.T) {
	app := New(testConfig())
	if e := app.Err(); e != nil {
		t.Fatal(e)
	}
}

// TestConfig rejects missing authority and endpoints that could send test credentials to AWS.
func TestConfig(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.ProviderID = "" }, func(c *Config) { c.ConsumerName = " " }, func(c *Config) { c.AWSRegion = "" }, func(c *Config) { c.InputQueueURL = "http://other/input.fifo" }, func(c *Config) { c.OutputQueueURL = c.InputQueueURL }, func(c *Config) { c.DatabaseURL = "postgres://u:secret@%invalid" }} {
		c := testConfig()
		change(&c)
		e := c.Validate()
		if e == nil {
			t.Fatal("invalid config accepted")
		}
		if strings.Contains(e.Error(), "secret") {
			t.Fatal("credential leaked")
		}
	}
	if _, e := LoadConfig(func(string) string { return "" }); e == nil {
		t.Fatal("empty environment accepted")
	}
}

// TestIDs checks UUID version/variant and independent identities for multiple instances.
func TestIDs(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, e := newID()
		if e != nil || len(id) != 36 || id[14] != '4' || !strings.ContainsRune("89ab", rune(id[19])) || seen[id] {
			t.Fatal("invalid or repeated UUID")
		}
		seen[id] = true
	}
}

// TestHTTPConfig rejects partially enabled or publicly exposed local HTTP configuration.
func TestHTTPConfig(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.HTTPAddress = "127.0.0.1:8080" },
		func(c *Config) { c.OIDCIssuer = "http://127.0.0.1:8081/realms/jungle-wallet" },
		func(c *Config) {
			c.HTTPAddress = "192.0.2.1:8080"
			c.OIDCIssuer = "http://localhost/realm"
			c.OIDCAudience = "wallet-api"
		},
	} {
		c := testConfig()
		change(&c)
		if c.Validate() == nil {
			t.Fatal("unsafe or incomplete HTTP configuration accepted")
		}
	}
	c := testConfig()
	c.HTTPAddress = "127.0.0.1:8080"
	c.OIDCIssuer = "http://127.0.0.1:8081/realms/jungle-wallet"
	c.OIDCAudience = "wallet-api"
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
}

// TestDeploymentConfig covers container DNS, wildcard bind and explicit credential loading.
func TestDeploymentConfig(t *testing.T) {
	env := map[string]string{
		"WALLET_DATABASE_URL":     "postgres://worker:password@postgres/db",
		"WALLET_SQS_ENDPOINT":     "http://localstack:4566",
		"WALLET_INPUT_QUEUE_URL":  "http://localstack:4566/queue/input.fifo",
		"WALLET_OUTPUT_QUEUE_URL": "http://localstack:4566/queue/output",
		"WALLET_PROVIDER_ID":      "a", "WALLET_CONSUMER_NAME": "stable",
		"WALLET_HTTP_ADDRESS": "0.0.0.0:8080", "WALLET_OIDC_ISSUER": "http://keycloak:8080/realms/test", "WALLET_OIDC_AUDIENCE": "wallet-api",
		"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "configured-key", "AWS_SECRET_ACCESS_KEY": "configured-secret", "AWS_SESSION_TOKEN": "configured-session",
	}
	c, err := LoadConfig(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	if c.AWSAccessKeyID != "configured-key" || c.AWSSecretAccessKey != "configured-secret" || c.AWSSessionToken != "configured-session" {
		t.Fatal("credential configuration lost")
	}
	for _, field := range []string{"AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		saved := env[field]
		delete(env, field)
		if _, err := LoadConfig(func(name string) string { return env[name] }); err == nil {
			t.Fatalf("missing %s accepted", field)
		}
		env[field] = saved
	}
	c.SQSEndpoint = "https://sqs.us-east-1.amazonaws.com"
	c.InputQueueURL = c.SQSEndpoint + "/123/input.fifo"
	c.OutputQueueURL = c.SQSEndpoint + "/123/output"
	if err := c.Validate(); err != nil {
		t.Fatal("explicit HTTPS deployment rejected")
	}
}
