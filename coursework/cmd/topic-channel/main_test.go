package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReceiptBoundary(t *testing.T) {
	b := broker{key: []byte(strings.Repeat("a", 32))}
	r := receipt{Topic: "topic", Channel: "channel", Subject: "$JS.ACK.B_topic.channel.1.1.1.1.0", Expires: time.Now().Add(time.Minute).Unix()}
	token := b.sign(r)
	if _, err := b.decode(token, "topic", "channel"); err != nil {
		t.Fatal(err)
	}
	other := broker{key: b.key, cluster: "another-cluster"}
	if _, err := other.decode(token, "topic", "channel"); err == nil {
		t.Fatal("accepted receipt for another cluster")
	}
	for _, wrong := range []struct{ token, topic, channel string }{{token, "other", "channel"}, {token, "topic", "other"}, {token + "x", "topic", "channel"}, {"arbitrary", "topic", "channel"}} {
		if _, err := b.decode(wrong.token, wrong.topic, wrong.channel); err == nil {
			t.Fatal("accepted invalid receipt")
		}
	}
	r.Expires = time.Now().Add(-time.Minute).Unix()
	if _, err := b.decode(b.sign(r), "topic", "channel"); err == nil {
		t.Fatal("accepted expired receipt")
	}
	r.Expires = time.Now().Add(time.Minute).Unix()
	r.Subject = "arbitrary.subject"
	if _, err := b.decode(b.sign(r), "topic", "channel"); err == nil {
		t.Fatal("accepted arbitrary subject")
	}
}

func TestHTTPBoundary(t *testing.T) {
	b := broker{key: []byte(strings.Repeat("a", 32)), busy: make(chan struct{}, 1)}
	for _, path := range []string{"/topics/bad.name", "/topics/a/channels/bad.name", "/topics/a/messages"} {
		r := httptest.NewRequest("POST", path, strings.NewReader(`{"data":"x","publisher_id":"bad.id"}`))
		r.Header.Set("Authorization", "Bearer "+string(b.key))
		w := httptest.NewRecorder()
		b.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("%s got %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	b.ServeHTTP(w, httptest.NewRequest("PUT", "/topics/a", nil))
	if w.Code != 401 {
		t.Fatal("unauthenticated request accepted")
	}
	b.busy <- struct{}{}
	r := httptest.NewRequest("PUT", "/topics/a", nil)
	r.Header.Set("Authorization", "Bearer "+string(b.key))
	w = httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 429 {
		t.Fatal("in-flight bound not enforced")
	}
	<-b.busy
	for _, data := range []string{`{"x":1}`, `{} {}`, strings.Repeat("x", 65537)} {
		var v struct{}
		if body(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(data)), &v) == nil {
			t.Fatal("invalid body accepted")
		}
	}
}
