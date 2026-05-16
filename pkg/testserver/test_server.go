package testserver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/app"
	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

// This is a drop-in replacement for the embedded etcd test server at:
// https://github.com/kubernetes/kubernetes/blob/v1.34.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/testserver/test_server.go

func NewTestConfig(t testing.TB) *embed.Config {
	cfg := embed.NewConfig()

	clientURL := url.URL{Scheme: "unix", Path: "/tmp/kine.sock"}

	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.ExperimentalWatchProgressNotifyInterval = 5 * time.Second
	cfg.Dir = t.TempDir()
	os.Chmod(cfg.Dir, 0700)
	return cfg
}

func RunEtcd(t testing.TB, cfg *embed.Config) *kubernetes.Client {
	t.Helper()
	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetOutput(t.Output())

	if cfg == nil {
		cfg = NewTestConfig(t)
	}

	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config := app.Config(nil)
	config.WaitGroup = wg
	config.Listener = cfg.ListenClientUrls[0].String()
	config.NotifyInterval = cfg.ExperimentalWatchProgressNotifyInterval
	config.CompactInterval = 0
	config.CompactMinRetain = 0

	// rqlite is a single-database server; isolate per test by spawning a dedicated
	// container instead of trying to derive a unique DSN from the static endpoint.
	if scheme, _, _ := strings.Cut(config.Endpoint, "://"); scheme == "rqlite" {
		config.Endpoint = startRqlite(t)
	}

	// use a unique database for each test
	dsn, err := setDatabasePath(config.Endpoint, cfg.Dir)
	if err != nil {
		t.Fatalf("Failed to set database path: %v", err)
	}
	config.Endpoint = dsn

	e, err := endpoint.Listen(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	tlsConfig, err := e.TLSConfig.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	// We don't have a ready channel, so just sleep for a bit
	time.Sleep(time.Second)

	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.KV = storagetesting.NewKVRecorder(client.KV)
	client.Kubernetes = storagetesting.NewKubernetesRecorder(client.Kubernetes)
	return client
}

func setDatabasePath(endpoint, dir string) (string, error) {
	// append a hash of the temp dir to the db name for remote engines where we cannot set the db path directly
	hash := fmt.Sprintf("%.8x", sha256.Sum256([]byte(dir)))
	scheme, _, _ := strings.Cut(endpoint, "://")
	switch scheme {
	case "memory":
		return endpoint, nil
	case "", "sqlite":
		// empty scheme means default sqlite db
		return fmt.Sprintf("sqlite://%s/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate", dir), nil
	case "nats", "jetstream":
		// nats doesn't have databases, so set the bucket param instead
		ep, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		v := ep.Query()
		v.Set("bucket", hash)
		ep.RawQuery = v.Encode()
		if ep.Host == "" {
			ep.Opaque = "//" + ep.Path
		}
		return ep.String(), nil
	case "mysql":
		//  mysql DSNs are not valid URLs, so just insert the hash before the query string, if any
		path, query, _ := strings.Cut(endpoint, "?")
		return path + hash + "?" + query, nil
	case "rqlite":
		// rqlite has no per-database routing; isolation is achieved by spawning a
		// dedicated rqlite container per test (see startRqlite). Pass the endpoint through.
		return endpoint, nil
	default:
		ep, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		ep.Path += hash
		return ep.String(), nil
	}
}

// startRqlite launches a single-node rqlite container for the lifetime of the test
// and returns an rqlite:// endpoint kine can connect to. The container is removed
// on test cleanup. This gives each rqlite-backed test a clean, isolated database,
// since rqlite is a single-database server and cannot be partitioned by DSN.
func startRqlite(t testing.TB) string {
	t.Helper()

	out, err := exec.Command("docker", "run", "-d", "--rm",
		"rqlite/rqlite",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to start rqlite container: %v\n%s", err, out)
	}
	containerID := strings.TrimSpace(string(out))

	t.Cleanup(func() {
		if stopOut, stopErr := exec.Command("docker", "stop", containerID).CombinedOutput(); stopErr != nil {
			t.Logf("docker stop %s: %v\n%s", containerID, stopErr, stopOut)
		}
	})

	inspectOut, err := exec.Command("docker", "container", "inspect",
		"-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
		containerID,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to inspect rqlite container %s: %v\n%s", containerID, err, inspectOut)
	}
	containerIP := strings.TrimSpace(string(inspectOut))

	t.Logf("docker rqlite container started as %s with IP %s", containerID, containerIP)

	statusURL := fmt.Sprintf("http://%s:4001/readyz", containerIP)
	deadline := time.Now().Add(120 * time.Second)
	for {
		resp, err := http.Get(statusURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			if psOut, psErr := exec.Command("docker", "ps").CombinedOutput(); psErr != nil {
				t.Logf("docker ps failed: %v\n%s", psErr, psOut)
			} else {
				t.Logf("docker ps: %s\n", psOut)
			}
			if logsOut, logsErr := exec.Command("docker", "logs", containerID).CombinedOutput(); logsErr != nil {
				t.Logf("docker logs failed: %v\n%s", logsErr, logsOut)
			} else {
				t.Logf("docker logs: %s\n", logsOut)
			}
			t.Fatalf("rqlite container %s did not become ready within 120s", containerID)
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Sprintf("rqlite://%s:4001", containerIP)
}
