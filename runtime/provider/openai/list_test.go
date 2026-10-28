package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestListModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"model-b"},{"id":"model-a"},{"id":""}]}`))
	}))
	defer srv.Close()

	ids, err := ListModels(context.Background(), Options{BaseURL: srv.URL, APIKey: "test-key", HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []string{"model-b", "model-a"}) {
		t.Fatalf("ids = %v（空 id 应跳过）", ids)
	}
}

func TestListModelsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()

	if _, err := ListModels(context.Background(), Options{BaseURL: srv.URL, HTTPClient: srv.Client()}); err == nil {
		t.Fatal("401 应报错")
	}
}
