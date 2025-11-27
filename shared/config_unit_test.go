package shared

import (
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
