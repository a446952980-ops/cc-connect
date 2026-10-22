package qq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"

	"github.com/gorilla/websocket"
)

func TestPlatform_Name(t *testing.T) {
	p := &Platform{}
	if got := p.Name(); got != "qq" {
		t.Errorf("Name() = %q, want %q", got, "qq")
	}
}

func TestNew_DefaultWSURL(t *testing.T) {
	p, err := New(map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://127.0.0.1:3001" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://127.0.0.1:3001")
	}
}

func TestNew_CustomWSURL(t *testing.T) {
	p, err := New(map[string]any{
		"ws_url": "ws://example.com:8080",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://example.com:8080" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://example.com:8080")
	}
}

func TestNew_WithToken(t *testing.T) {
	p, err := New(map[string]any{
		"token": "my-secret-token",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.token != "my-secret-token" {
		t.Errorf("token = %q, want %q", platform.token, "my-secret-token")
	}
}

func TestNew_WithAllowFrom(t *testing.T) {
	p, err := New(map[string]any{
		"allow_from": "user1,user2,*",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.allowFrom != "user1,user2,*" {
		t.Errorf("allowFrom = %q, want %q", platform.allowFrom, "user1,user2,*")
	}
}

func TestNew_ShareSessionInChannel(t *testing.T) {
	p, err := New(map[string]any{
		"share_session_in_channel": true,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if !platform.shareSessionInChannel {
		t.Error("shareSessionInChannel = false, want true")
	}
}

// TestHandleMessage_GroupRequiresAtBot verifies that group messages are only
// processed when they explicitly @-mention the bot, while private messages
// pass without a mention. Regression test for the group spam gate: before this
// change any group member could trigger the bot without mentioning it.
func TestHandleMessage_GroupRequiresAtBot(t *testing.T) {
	const botUserID = 999999
	const otherUserID = 777777

	upgrader := websocket.Upgrader{}
	events := make(chan []byte, 16) // server → platform (bot events)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		// Single writer: gorilla/websocket forbids concurrent writers on one
		// connection, so both pushed test events and API responses funnel
		// through this channel into one goroutine.
		out := make(chan []byte, 32)
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case raw := <-out:
					if c.WriteMessage(websocket.TextMessage, raw) != nil {
						return
					}
				case <-done:
					return
				}
			}
		}()
		stopEvents := make(chan struct{})
		defer close(stopEvents)
		go func() {
			for {
				select {
				case raw := <-events:
					select {
					case out <- raw:
					case <-stopEvents:
						return
					}
				case <-stopEvents:
					return
				}
			}
		}()

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]any
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			echo, _ := req["echo"].(string)
			var data any
			switch req["action"] {
			case "get_login_info":
				data = map[string]any{"user_id": botUserID, "nickname": "TestBot"}
			case "get_group_info":
				data = map[string]any{"group_id": 100, "group_name": "TestGroup"}
			default:
				continue
			}
			respRaw, _ := json.Marshal(map[string]any{
				"status": "ok", "retcode": 0, "echo": echo, "data": data,
			})
			out <- respRaw
		}
	}))
	defer ts.Close()

	received := make(chan *core.Message, 8)
	p := &Platform{
		wsURL: "ws" + strings.TrimPrefix(ts.URL, "http"),
	}
	if err := p.Start(func(_ core.Platform, m *core.Message) {
		received <- m
	}); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer p.Stop()

	if p.selfID != botUserID {
		t.Fatalf("selfID = %d, want %d", p.selfID, botUserID)
	}

	push := func(event map[string]any) {
		raw, _ := json.Marshal(event)
		events <- raw
	}
	groupEvent := func(id int64, message []any) map[string]any {
		return map[string]any{
			"post_type": "message", "message_type": "group",
			"user_id": otherUserID, "group_id": 100, "message_id": id,
			"message": message,
		}
	}
	atBot := map[string]any{"type": "at", "data": map[string]any{"qq": "999999"}}
	atOther := map[string]any{"type": "at", "data": map[string]any{"qq": "111222"}}
	text := map[string]any{"type": "text", "data": map[string]any{"text": "hello"}}

	// 1. Group message without any @mention → must be ignored.
	push(groupEvent(1, []any{text}))

	// 2. Group message @-mentioning a different user → must be ignored.
	push(groupEvent(2, []any{atOther, text}))

	// 3. Group message @-mentioning the bot (string qq, NapCat form) → accepted.
	push(groupEvent(3, []any{atBot, text}))

	// 4. Group message @-mentioning the bot (numeric qq, other implementations).
	push(groupEvent(4, []any{map[string]any{"type": "at", "data": map[string]any{"qq": float64(botUserID)}}, text}))

	// 5. Private message without @mention → accepted (gate is group-only).
	push(map[string]any{
		"post_type": "message", "message_type": "private",
		"user_id": otherUserID, "message_id": 5, "message": []any{text},
	})

	// Expect exactly the messages from cases 3, 4 and 5.
	wantIDs := map[int64]bool{3: true, 4: true, 5: true}
	deadline := time.After(5 * time.Second)
	got := map[int64]bool{}
	for len(got) < len(wantIDs) {
		select {
		case m := <-received:
			id, err := strconv.ParseInt(m.MessageID, 10, 64)
			if err != nil {
				t.Fatalf("unexpected message id %q", m.MessageID)
			}
			if !wantIDs[id] {
				t.Fatalf("message %d should not have been processed (no @bot)", id)
			}
			got[id] = true
		case <-deadline:
			for id := range wantIDs {
				if !got[id] {
					t.Fatalf("message %d (@bot) was not processed", id)
				}
			}
			t.Fatal("timed out waiting for @bot messages")
		}
	}
	// Drain briefly to catch any wrongly processed no-@ group message.
	select {
	case m := <-received:
		t.Fatalf("unexpected extra message processed: %q", m.MessageID)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestResolveGroupName_NoConn_ReturnsFallbackImmediately is the regression
// test for the readLoop deadlock: resolveGroupName used to call get_group_info
// inline from handleMessage, but callAPI's echo response is routed by the same
// readLoop goroutine — blocking every incoming group message for the full 15s
// API timeout. The lookup must be async now, and must not panic without a
// connection.
func TestResolveGroupName_NoConn_ReturnsFallbackImmediately(t *testing.T) {
	p := &Platform{}
	start := time.Now()
	got := p.resolveGroupName(123456)
	elapsed := time.Since(start)
	if got != "123456" {
		t.Fatalf("resolveGroupName = %q, want fallback %q", got, "123456")
	}
	if elapsed > time.Second {
		t.Fatalf("resolveGroupName blocked for %v; group info lookup must be async", elapsed)
	}
}

// TestIsAtBot_CQCodeStringFallback verifies the CQ-code string path of
// isAtBot, including that a longer QQ ID sharing the target's prefix does not
// match (boundary check on the trailing , or ]).
func TestIsAtBot_CQCodeStringFallback(t *testing.T) {
	p := &Platform{selfID: 12345}
	cases := []struct {
		name string
		raw  map[string]any
		want bool
	}{
		{"exact match", map[string]any{"raw_message": "[CQ:at,qq=12345] hi"}, true},
		{"match with attrs", map[string]any{"raw_message": "text [CQ:at,qq=12345,text=@bot] rest"}, true},
		{"prefix collision", map[string]any{"raw_message": "[CQ:at,qq=123456] hi"}, false},
		{"other user", map[string]any{"raw_message": "[CQ:at,qq=54321] hi"}, false},
		{"no mention", map[string]any{"raw_message": "just text"}, false},
		{"message field string", map[string]any{"message": "[CQ:at,qq=12345] hi"}, true},
	}
	for _, tc := range cases {
		if got := p.isAtBot(tc.raw); got != tc.want {
			t.Errorf("%s: isAtBot = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStart_FetchesSelfIDWithoutTimeout verifies that Start() completes
// promptly with selfID populated from the get_login_info OneBot API call.
// Regression for a bug where Start invoked callAPI BEFORE launching readLoop,
// so the API response had no consumer and callAPI always timed out after 15s
// — leaving selfID=0 and disabling the self-message filter in handleMessage.
func TestStart_FetchesSelfIDWithoutTimeout(t *testing.T) {
	const botUserID = 999999

	upgrader := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]any
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			if req["action"] == "get_login_info" {
				echo, _ := req["echo"].(string)
				resp := map[string]any{
					"status":  "ok",
					"retcode": 0,
					"echo":    echo,
					"data":    map[string]any{"user_id": botUserID, "nickname": "TestBot"},
				}
				raw, _ := json.Marshal(resp)
				_ = c.WriteMessage(websocket.TextMessage, raw)
			}
		}
	}))
	defer ts.Close()

	p := &Platform{
		wsURL: "ws" + strings.TrimPrefix(ts.URL, "http"),
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Start(func(core.Platform, *core.Message) {})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = p.Stop()
		t.Fatal("Start did not complete within 5s; readLoop likely starts after callAPI, so get_login_info never gets a response")
	}
	defer p.Stop()

	if p.selfID != botUserID {
		t.Errorf("selfID = %d, want %d (self-message filter would be disabled)", p.selfID, botUserID)
	}
}
