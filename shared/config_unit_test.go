package shared

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestConvertToAWSConfigOptions(t *testing.T) {
	orig := []string{"some-string", "other-string"}
	var arg []any
	for _, v := range orig {
		arg = append(arg, v)
	}
	t.Run("negative", func(t *testing.T) {
		_, err := ConvertToAWSConfigOptions[float32](arg)
		if err == nil {
			t.Fatal("expected error, got none")
		}
	})
	t.Run("positive", func(t *testing.T) {
		res, err := ConvertToAWSConfigOptions[string](arg)
		if err != nil {
			t.Fatal(err)
		}

		if len(res) != len(orig) {
			t.Fatalf("want %d values, got %d", len(orig), len(res))
		}

		for i := range res {
			if res[i] != orig[i] {
				t.Fatalf("want %s, got %s", orig[i], res[i])
			}
		}
	})
}

func TestWithHTTPClientTimeout(t *testing.T) {
	cfg := NewDefaultConfig()
	timeout := 3 * time.Second

	WithHTTPClientTimeout(timeout)(cfg)
	if cfg.HTTPClientTimeout != timeout {
		t.Fatalf("Config HTTPClientTimeout = %s, want %s", cfg.HTTPClientTimeout, timeout)
	}

	alnCfg := NewDefaultALNConfig()
	for _, opt := range cfg.ToALNOptions() {
		opt(&alnCfg)
	}

	if alnCfg.HTTPClientTimeout != timeout {
		t.Fatalf("ALNConfig HTTPClientTimeout = %s, want %s", alnCfg.HTTPClientTimeout, timeout)
	}
}

func TestDefaultConfigsUseSeparateTLSSessionCaches(t *testing.T) {
	cfg1 := NewDefaultConfig()
	cfg2 := NewDefaultConfig()
	if cfg1.TLSSessionCache == cfg2.TLSSessionCache {
		t.Fatal("NewDefaultConfig should create a separate TLS session cache per config")
	}

	alnCfg1 := NewDefaultALNConfig()
	alnCfg2 := NewDefaultALNConfig()
	if alnCfg1.TLSSessionCache == alnCfg2.TLSSessionCache {
		t.Fatal("NewDefaultALNConfig should create a separate TLS session cache per config")
	}
}

func TestWithResponseCompression(t *testing.T) {
	cfg := NewDefaultConfig()
	if len(cfg.ResponseCompression) != 0 {
		t.Fatalf("default ResponseCompression = %v, want empty", cfg.ResponseCompression)
	}

	WithResponseCompression(ResponseCompressionDeflate)(cfg)
	want := []ResponseCompression{ResponseCompressionDeflate}
	if !reflect.DeepEqual(cfg.ResponseCompression, want) {
		t.Fatalf("ResponseCompression = %v, want %v", cfg.ResponseCompression, want)
	}

	WithoutResponseCompression()(cfg)
	if len(cfg.ResponseCompression) != 0 {
		t.Fatalf("ResponseCompression = %v, want empty", cfg.ResponseCompression)
	}

	transport, ok := NewHTTPTransport(*cfg).(*http.Transport)
	if !ok {
		t.Fatalf("NewHTTPTransport returned %T, want *http.Transport", transport)
	}
	if !transport.DisableCompression {
		t.Fatal("DisableCompression should be true when response compression is disabled")
	}
}
