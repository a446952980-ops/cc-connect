package qq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"

	"github.com/gorilla/websocket"
)

type stampedReq struct {
	Action string
	Params map[string]any
	At     time.Time
}

// startFakeOneBot runs a fake OneBot WS server that records every request
// (except get_login_info style reads) in arrival order and acks it.
func startFakeOneBot(t *testing.T) (*Platform, func() []stampedReq) {
	t.Helper()
	var mu sync.Mutex
	var got []stampedReq

	upgrader := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, raw, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]any
			if json.Unmarshal(raw, &req) != nil {
				continue
			}
			act, _ := req["action"].(string)
			echo, _ := req["echo"].(string)
			if act != "get_login_info" && act != "get_group_info" {
				params, _ := req["params"].(map[string]any)
				mu.Lock()
				got = append(got, stampedReq{Action: act, Params: params, At: time.Now()})
				mu.Unlock()
			}
			resp, _ := json.Marshal(map[string]any{"status": "ok", "retcode": 0, "echo": echo, "data": map[string]any{}})
			if c.WriteMessage(websocket.TextMessage, resp) != nil {
				return
			}
		}
	}))
	t.Cleanup(ts.Close)

	p := &Platform{wsURL: "ws" + strings.TrimPrefix(ts.URL, "http")}
	if err := p.Start(func(core.Platform, *core.Message) {}); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	return p, func() []stampedReq {
		mu.Lock()
		defer mu.Unlock()
		return append([]stampedReq(nil), got...)
	}
}

func TestQueue_SpacesMessagesAndKeepsOrder(t *testing.T) {
	p, reqs := startFakeOneBot(t)
	p.sendIntervalMin = 150 * time.Millisecond
	p.sendIntervalMax = 250 * time.Millisecond

	rctx := &replyContext{messageType: "group", groupID: 100, userID: 7, messageID: 555}

	for _, text := range []string{"one", "two", "three"} {
		if err := p.Send(nil, rctx, text); err != nil {
			t.Fatalf("Send(%s): %v", text, err)
		}
	}
	// Final reply and done reaction come after the side messages, in order.
	if err := p.SendFinalReply(nil, rctx, "final"); err != nil {
		t.Fatalf("SendFinalReply: %v", err)
	}
	p.doneEmojiID = 1
	p.AddDoneReaction(rctx)
	deadline := time.Now().Add(2 * time.Second)
	for len(reqs()) < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	got := reqs()
	if len(got) != 5 {
		t.Fatalf("got %d requests, want 5: %+v", len(got), got)
	}
	wantActions := []string{"send_group_msg", "send_group_msg", "send_group_msg", "send_group_msg", "set_msg_emoji_like"}
	for i, a := range wantActions {
		if got[i].Action != a {
			t.Errorf("request %d action = %s, want %s", i, got[i].Action, a)
		}
	}
	for i := 1; i < 4; i++ {
		if gap := got[i].At.Sub(got[i-1].At); gap < 140*time.Millisecond {
			t.Errorf("gap between message %d and %d = %v, want >= 150ms", i-1, i, gap)
		}
	}
}

func TestQueue_ConcurrentSendsAreSerialized(t *testing.T) {
	p, reqs := startFakeOneBot(t)
	p.sendIntervalMin = 80 * time.Millisecond
	p.sendIntervalMax = 80 * time.Millisecond

	rctx := &replyContext{messageType: "group", groupID: 100}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Send(nil, rctx, "x")
		}()
	}
	wg.Wait()

	got := reqs()
	if len(got) != 4 {
		t.Fatalf("got %d requests, want 4", len(got))
	}
	for i := 1; i < len(got); i++ {
		if gap := got[i].At.Sub(got[i-1].At); gap < 70*time.Millisecond {
			t.Errorf("gap %d = %v, want >= 80ms", i, gap)
		}
	}
}

func TestSendFinalReply_SplitsLongAnswer(t *testing.T) {
	p, reqs := startFakeOneBot(t)
	p.finalMaxChars = 20
	p.sendIntervalMin, p.sendIntervalMax = 10*time.Millisecond, 20*time.Millisecond

	rctx := &replyContext{messageType: "group", groupID: 100, messageID: 9}
	text := "第一句话说完了。第二句话也说完了。第三句话还没完呢。第四句话到此为止。"
	if err := p.SendFinalReply(nil, rctx, text); err != nil {
		t.Fatalf("SendFinalReply: %v", err)
	}

	got := reqs()
	if len(got) < 2 {
		t.Fatalf("expected the answer to be split, got %d message(s)", len(got))
	}
	if _, ok := got[0].Params["message"].([]any); !ok {
		t.Errorf("first chunk should carry the reply segment, got %#v", got[0].Params["message"])
	}
	for i, r := range got[1:] {
		if _, ok := r.Params["message"].(string); !ok {
			t.Errorf("chunk %d should be a plain string, got %#v", i+1, r.Params["message"])
		}
	}
}

func TestSplitReply(t *testing.T) {
	if got := splitReply("短消息", 120); len(got) != 1 || got[0] != "短消息" {
		t.Errorf("short text changed: %#v", got)
	}
	if got := splitReply("很长的文字", 0); len(got) != 1 {
		t.Errorf("max=0 must not split: %#v", got)
	}
	long := strings.Repeat("字", 300)
	chunks := splitReply(long, 120)
	if len(chunks) != 3 {
		t.Fatalf("300 runes / 120 = %d chunks, want 3", len(chunks))
	}
	for i, c := range chunks {
		if n := len([]rune(c)); n > 120 {
			t.Errorf("chunk %d has %d runes, want <= 120", i, n)
		}
	}
	para := splitReply("第一段\n\n第二段", 4)
	if len(para) != 2 || para[0] != "第一段" || para[1] != "第二段" {
		t.Errorf("paragraphs not preserved: %#v", para)
	}
}

func TestDoAction_WhitelistAndDefaults(t *testing.T) {
	p, reqs := startFakeOneBot(t)
	rctx := &replyContext{messageType: "group", groupID: 100, userID: 7}

	if _, err := p.DoAction(nil, rctx, "set_group_ban", map[string]any{"user_id": 1}); err == nil {
		t.Error("set_group_ban must be rejected")
	}
	if _, err := p.DoAction(nil, rctx, "group_poke", map[string]any{"user_id": 42}); err != nil {
		t.Fatalf("group_poke: %v", err)
	}
	if _, err := p.DoAction(nil, rctx, "set_input_status", map[string]any{"status": 1}); err != nil {
		t.Fatalf("set_input_status: %v", err)
	}

	got := reqs()
	if len(got) != 2 {
		t.Fatalf("got %d requests, want 2 (rejected action must not be sent)", len(got))
	}
	if got[0].Action != "group_poke" || got[0].Params["group_id"] != float64(100) || got[0].Params["user_id"] != float64(42) {
		t.Errorf("group_poke params = %#v", got[0].Params)
	}
	if got[1].Params["group_id"] != float64(100) {
		t.Errorf("set_input_status should default group_id, got %#v", got[1].Params)
	}
}
