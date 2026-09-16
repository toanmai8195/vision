package main

import (
	"log/slog"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	catalogv1 "com.tm/vision/com/tm/proto/vision/catalog/v1"
	eventv1 "com.tm/vision/com/tm/proto/vision/event/v1"
)

func base() *eventv1.DataEvent {
	return &eventv1.DataEvent{EventId: "e-1", UserId: "U1001", Attr: "a", EventTsMs: 1789441200000}
}

func tagEvent(add, remove []string) *eventv1.DataEvent {
	ev := base()
	ev.Payload = &eventv1.DataEvent_Tag{Tag: &eventv1.TagEvent{TagsAdd: add, TagsRemove: remove}}
	return ev
}

func valueEvent(v string) *eventv1.DataEvent {
	ev := base()
	ev.Payload = &eventv1.DataEvent_Value{Value: &eventv1.ValueEvent{Value: v}}
	return ev
}

func tagValueEvent(tag, v string) *eventv1.DataEvent {
	ev := base()
	ev.Payload = &eventv1.DataEvent_TagValue{TagValue: &eventv1.TagValueEvent{Tag: tag, Value: v}}
	return ev
}

func TestValidate(t *testing.T) {
	missingUser := tagEvent([]string{"hcm"}, nil)
	missingUser.UserId = ""

	tests := []struct {
		name    string
		ev      *eventv1.DataEvent
		wantErr bool
	}{
		{"tag add", tagEvent([]string{"hcm"}, nil), false},
		{"tag remove only", tagEvent(nil, []string{"paylater"}), false},
		{"tag empty", tagEvent(nil, nil), true},
		{"tag add and remove same", tagEvent([]string{"a"}, []string{"a"}), true},
		{"value int", valueEvent("55000"), false},
		{"value decimal", valueEvent("-12.500001"), false},
		{"value too many decimals", valueEvent("1.1234567"), true},
		{"value not number", valueEvent("abc"), true},
		{"tag_value ok", tagValueEvent("fnb", "45000"), false},
		{"tag_value missing tag", tagValueEvent("", "45000"), true},
		{"missing payload", base(), true},
		{"missing user", missingUser, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestPayloadMatchesDataType(t *testing.T) {
	tests := []struct {
		name    string
		dt      catalogv1.DataType
		ev      *eventv1.DataEvent
		wantErr bool
	}{
		{"MUTEX single add", catalogv1.DataType_MUTEX, tagEvent([]string{"hn"}, nil), false},
		{"MUTEX two adds", catalogv1.DataType_MUTEX, tagEvent([]string{"hn", "hcm"}, nil), true},
		{"MUTEX value", catalogv1.DataType_MUTEX, valueEvent("1"), true},
		{"NOT_MUTEX multi add", catalogv1.DataType_NOT_MUTEX, tagEvent([]string{"paylater", "insurance"}, nil), false},
		{"NOT_MUTEX tag_value", catalogv1.DataType_NOT_MUTEX, tagValueEvent("fnb", "1"), true},
		{"PARTIAL_VALUE value", catalogv1.DataType_PARTIAL_VALUE, valueEvent("1255000"), false},
		{"PARTIAL_VALUE tag", catalogv1.DataType_PARTIAL_VALUE, tagEvent([]string{"x"}, nil), true},
		{"PARTIAL_VALUE_BY_TAG tag_value", catalogv1.DataType_PARTIAL_VALUE_BY_TAG, tagValueEvent("fnb", "600000"), false},
		{"PARTIAL_VALUE_BY_TAG value", catalogv1.DataType_PARTIAL_VALUE_BY_TAG, valueEvent("1"), true},
		{"UNSPECIFIED", catalogv1.DataType_DATA_TYPE_UNSPECIFIED, valueEvent("1"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := PayloadMatchesDataType(tc.dt, tc.ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("PayloadMatchesDataType() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestHTTP(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := prometheus.NewRegistry()
	registry.MustRegister(eventsTotal)
	srv := httptest.NewServer(newMux(logger, registry))
	defer srv.Close()

	cases := []struct {
		body string
		want int
	}{
		{`{"eventId":"e-9001","userId":"U1001","attr":"txn_category","eventTsMs":"1789441200000","tag":{"tagsAdd":["fnb"]}}`, http.StatusAccepted},
		{`{"eventId":"e-9002","userId":"U1001","attr":"txn_amount","eventTsMs":"1789441200000","value":{"value":"1200000"}}`, http.StatusAccepted},
		{`{"eventId":"e-9003","userId":"U1002","attr":"txn_amount_by_category","eventTsMs":"1789441200000","tagValue":{"tag":"fnb","value":"45000"}}`, http.StatusAccepted},
		{`{"eventId":"e-bad","userId":"U1002","attr":"txn_amount","eventTsMs":"1789441200000","value":{"value":"x"}}`, http.StatusBadRequest},
		{`not json`, http.StatusBadRequest},
	}
	for _, c := range cases {
		resp, err := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("POST %s → %d, want %d", c.body, resp.StatusCode, c.want)
		}
	}

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	metrics, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(metrics), `vision_collector_events_total{payload="tag_value",result="accepted"} 1`) {
		t.Errorf("metrics missing tag_value counter:\n%s", metrics)
	}
}
