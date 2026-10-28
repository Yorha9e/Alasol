package main

import "testing"

// TestResolveModel：模型名解析口径——flag 优先、env 兜底、**无错误默认**（fail-close）。
func TestResolveModel(t *testing.T) {
	t.Setenv("ALASOL_MODEL", "")
	if _, err := resolveModel(""); err == nil {
		t.Fatal("flag 与 env 都空时应报错（不做错误默认）")
	}

	t.Setenv("ALASOL_MODEL", "env-model")
	got, err := resolveModel("")
	if err != nil || got != "env-model" {
		t.Fatalf("env 兜底: got %q err %v, want env-model", got, err)
	}

	got, err = resolveModel("flag-model")
	if err != nil || got != "flag-model" {
		t.Fatalf("flag 优先: got %q err %v, want flag-model", got, err)
	}
}
