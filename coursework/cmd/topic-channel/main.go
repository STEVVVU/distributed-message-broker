package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,48}$`)
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,96}$`)

type broker struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	key     []byte
	cluster string
	busy    chan struct{}
}

type receipt struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
	Subject string `json:"subject"`
	Expires int64  `json:"expires"`
	Cluster string `json:"cluster"`
}

func (b *broker) sign(r receipt) string {
	r.Cluster = b.cluster
	data, _ := json.Marshal(r)
	mac := hmac.New(sha256.New, b.key)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (b *broker) decode(token, topic, channel string) (receipt, error) {
	var r receipt
	parts := strings.Split(token, ".")
	if len(token) > 2048 || len(parts) != 2 {
		return r, errors.New("invalid receipt")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return r, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return r, err
	}
	mac := hmac.New(sha256.New, b.key)
	mac.Write(data)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return r, errors.New("invalid signature")
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, err
	}
	if r.Cluster != b.cluster || r.Topic != topic || r.Channel != channel || r.Expires < time.Now().Unix() || !strings.HasPrefix(r.Subject, "$JS.ACK.B_"+topic+"."+channel+".") {
		return r, errors.New("receipt namespace or expiry mismatch")
	}
	return r, nil
}

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func body(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("one JSON object required")
	}
	return nil
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+string(b.key))) != 1 {
		write(w, 401, map[string]string{"error": "bearer capability required"})
		return
	}
	select {
	case b.busy <- struct{}{}:
		defer func() { <-b.busy }()
	default:
		write(w, 429, map[string]string{"error": "in-flight limit reached"})
		return
	}
	p := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(p) < 2 || p[0] != "topics" || !namePattern.MatchString(p[1]) || (len(p) > 3 && p[2] == "channels" && !namePattern.MatchString(p[3])) {
		write(w, 400, map[string]string{"error": "invalid topic/channel path"})
		return
	}
	topic, stream := p[1], "B_"+p[1]
	var result any
	var err error
	switch {
	case len(p) == 2 && r.Method == "PUT":
		_, err = b.js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{"course." + topic}, Storage: nats.FileStorage, Replicas: 3, Retention: nats.LimitsPolicy, MaxMsgs: 100000, MaxBytes: 32 << 20, MaxMsgSize: 32768, MaxConsumers: 64, Discard: nats.DiscardNew, Duplicates: 2 * time.Minute})
		result = map[string]string{"topic": topic}
	case len(p) == 3 && p[2] == "messages" && r.Method == "POST":
		var input struct {
			Data        string `json:"data"`
			PublisherID string `json:"publisher_id"`
		}
		if body(w, r, &input) != nil || len(input.Data) == 0 || len(input.Data) > 32768 || !idPattern.MatchString(input.PublisherID) {
			write(w, 400, map[string]string{"error": "data must be 1..32768 bytes and publisher_id 1..96 safe characters"})
			return
		}
		var ack *nats.PubAck
		ack, err = b.js.Publish("course."+topic, []byte(input.Data), nats.MsgId(input.PublisherID))
		if err == nil {
			result = map[string]any{"sequence": ack.Sequence, "duplicate": ack.Duplicate}
		}
	case len(p) == 4 && p[2] == "channels" && r.Method == "PUT":
		_, err = b.js.AddConsumer(stream, &nats.ConsumerConfig{Durable: p[3], AckPolicy: nats.AckExplicitPolicy, AckWait: 2 * time.Second, DeliverPolicy: nats.DeliverAllPolicy, ReplayPolicy: nats.ReplayInstantPolicy, MaxAckPending: 128, MaxWaiting: 32, MaxRequestBatch: 16, MaxRequestExpires: time.Second, Replicas: 3})
		result = map[string]string{"channel": p[3]}
	case len(p) == 5 && p[2] == "channels" && p[4] == "pull" && r.Method == "POST":
		var subscription *nats.Subscription
		subscription, err = b.js.PullSubscribe("", p[3], nats.Bind(stream, p[3]))
		if err == nil {
			defer subscription.Unsubscribe()
			var messages []*nats.Msg
			messages, err = subscription.Fetch(1, nats.MaxWait(750*time.Millisecond))
			if errors.Is(err, nats.ErrTimeout) {
				err = nil
				result = map[string]any{"messages": []any{}}
			} else if err == nil {
				message := messages[0]
				var metadata *nats.MsgMetadata
				metadata, err = message.Metadata()
				if err == nil {
					result = map[string]any{"messages": []any{map[string]any{"data": string(message.Data), "sequence": metadata.Sequence.Stream, "attempt": metadata.NumDelivered, "receipt": b.sign(receipt{Topic: topic, Channel: p[3], Subject: message.Reply, Expires: time.Now().Add(10 * time.Minute).Unix()})}}}
				}
			}
		}
	case len(p) == 5 && p[2] == "channels" && p[4] == "ack" && r.Method == "POST":
		var input struct {
			Receipt string `json:"receipt"`
		}
		if body(w, r, &input) != nil {
			write(w, 400, map[string]string{"error": "invalid ACK body"})
			return
		}
		var parsed receipt
		parsed, err = b.decode(input.Receipt, topic, p[3])
		if err != nil {
			write(w, 400, map[string]string{"error": "invalid receipt"})
			return
		}
		_, err = b.nc.Request(parsed.Subject, []byte("+ACK"), 2*time.Second)
		result = map[string]bool{"acknowledged": true}
	default:
		write(w, 404, map[string]string{"error": "unknown operation"})
		return
	}
	if err != nil {
		write(w, 503, map[string]string{"error": "broker operation unavailable", "detail": err.Error()})
		return
	}
	write(w, 200, result)
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "loopback HTTP address")
	servers := flag.String("servers", "nats://127.0.0.1:4222", "comma-separated loopback NATS servers")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("HTTP listen must be a numeric loopback address")
	}
	for _, server := range strings.Split(*servers, ",") {
		address := strings.TrimPrefix(server, "nats://")
		h, _, e := net.SplitHostPort(address)
		if e != nil || net.ParseIP(h) == nil || !net.ParseIP(h).IsLoopback() {
			log.Fatal("NATS servers must be numeric loopback nats:// addresses")
		}
	}
	key := os.Getenv("BROKER_TOKEN")
	if len(key) < 32 {
		log.Fatal("BROKER_TOKEN must contain at least 32 random characters")
	}
	nc, err := nats.Connect(*servers, nats.Token(os.Getenv("NATS_TOKEN")), nats.Timeout(2*time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.ReconnectBufSize(0))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(2 * time.Second))
	if err != nil {
		log.Fatal(err)
	}
	cluster := sha256.Sum256([]byte(*servers))
	server := &http.Server{Addr: *listen, Handler: &broker{nc: nc, js: js, key: []byte(key), cluster: base64.RawURLEncoding.EncodeToString(cluster[:]), busy: make(chan struct{}, 32)}, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8192}
	fmt.Println("topic/channel HTTP ready", *listen)
	log.Fatal(server.ListenAndServe())
}
