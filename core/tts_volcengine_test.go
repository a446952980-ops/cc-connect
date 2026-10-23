package core

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// volcServerFrame builds a server frame: header, flags/event, session id,
// payload size and payload, as the real service sends them.
func volcServerFrame(msgType byte, event int32, payload []byte) []byte {
	b := []byte{0x11, msgType<<4 | volcFlagEvent, 0x10, 0x00}
	b = binary.BigEndian.AppendUint32(b, uint32(event))
	sid := []byte("sess-1")
	b = binary.BigEndian.AppendUint32(b, uint32(len(sid)))
	b = append(b, sid...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func volcErrorFrame(code uint32, msg string) []byte {
	b := []byte{0x11, volcMsgError << 4, 0x10, 0x00}
	b = binary.BigEndian.AppendUint32(b, code)
	b = binary.BigEndian.AppendUint32(b, uint32(len(msg)))
	return append(b, msg...)
}

type volcCapture struct {
	mu     sync.Mutex
	header http.Header
	req    map[string]any
	raw    []byte
}

func startFakeVolc(t *testing.T, reply func(c *websocket.Conn)) (*VolcengineTTS, *volcCapture) {
	t.Helper()
	cap := &volcCapture{}
	up := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		cap.mu.Lock()
		cap.header, cap.raw = r.Header.Clone(), raw
		n := binary.BigEndian.Uint32(raw[4:8])
		_ = json.Unmarshal(raw[8:8+n], &cap.req)
		cap.mu.Unlock()
		reply(c)
	}))
	t.Cleanup(ts.Close)
	v := NewVolcengineTTS("app-1", "tok-1", "", "ws"+strings.TrimPrefix(ts.URL, "http"))
	v.Timeout = 3 * time.Second
	return v, cap
}

func TestVolcengineTTS_AssemblesAudio(t *testing.T) {
	v, cap := startFakeVolc(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(0x9, 350, []byte(`{}`)))
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(volcMsgAudioOnly, 352, []byte("AAA")))
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(volcMsgAudioOnly, 352, []byte("BBB")))
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(0x9, 152, []byte(`{}`)))
	})

	audio, format, err := v.Synthesize(context.Background(), "你好", TTSSynthesisOpts{Voice: "spk-1", Speed: 1.5})
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	if string(audio) != "AAABBB" || format != "mp3" {
		t.Errorf("audio = %q format = %q, want AAABBB / mp3", audio, format)
	}

	if got := cap.header.Get("X-Api-App-Id"); got != "app-1" {
		t.Errorf("app id header = %q", got)
	}
	if got := cap.header.Get("X-Api-Access-Key"); got != "tok-1" {
		t.Errorf("access key header = %q", got)
	}
	if got := cap.header.Get("X-Api-Resource-Id"); got != "seed-tts-2.0" {
		t.Errorf("resource id header = %q, want default seed-tts-2.0", got)
	}
	if cap.header.Get("X-Api-Connect-Id") == "" {
		t.Error("connect id header missing")
	}
	if string(cap.raw[:4]) != "\x11\x10\x10\x00" {
		t.Errorf("request header = % x, want 11 10 10 00", cap.raw[:4])
	}
	rp := cap.req["req_params"].(map[string]any)
	if rp["text"] != "你好" || rp["speaker"] != "spk-1" {
		t.Errorf("req_params = %#v", rp)
	}
	ap := rp["audio_params"].(map[string]any)
	if ap["format"] != "mp3" || ap["sample_rate"] != float64(24000) || ap["speech_rate"] != float64(50) {
		t.Errorf("audio_params = %#v, want mp3/24000/speech_rate 50", ap)
	}
}

func TestVolcengineTTS_DefaultSpeakerAndSpeedClamp(t *testing.T) {
	v, cap := startFakeVolc(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(volcMsgAudioOnly, 352, []byte("A")))
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(0x9, 152, []byte(`{}`)))
	})
	if _, _, err := v.Synthesize(context.Background(), "x", TTSSynthesisOpts{Speed: 9}); err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	rp := cap.req["req_params"].(map[string]any)
	if rp["speaker"] != volcengineDefaultSpeaker {
		t.Errorf("speaker = %v, want default", rp["speaker"])
	}
	if got := rp["audio_params"].(map[string]any)["speech_rate"]; got != float64(100) {
		t.Errorf("speech_rate = %v, want clamped 100", got)
	}
}

func TestVolcengineTTS_ServerErrors(t *testing.T) {
	v, _ := startFakeVolc(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, volcErrorFrame(45000000, `{"error":"bad speaker"}`))
	})
	_, _, err := v.Synthesize(context.Background(), "x", TTSSynthesisOpts{})
	if err == nil || !strings.Contains(err.Error(), "45000000") || !strings.Contains(err.Error(), "bad speaker") {
		t.Errorf("error = %v, want code and message", err)
	}

	v, _ = startFakeVolc(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(0x9, 152, []byte(`{}`)))
	})
	if _, _, err := v.Synthesize(context.Background(), "x", TTSSynthesisOpts{}); err == nil || !strings.Contains(err.Error(), "no audio") {
		t.Errorf("error = %v, want no audio", err)
	}

	v, _ = startFakeVolc(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, volcServerFrame(0x9, 153, []byte(`{"message":"quota"}`)))
	})
	if _, _, err := v.Synthesize(context.Background(), "x", TTSSynthesisOpts{}); err == nil || !strings.Contains(err.Error(), "quota") {
		t.Errorf("error = %v, want session failure with message", err)
	}
}

func TestVolcengineTTS_CancelAndTimeout(t *testing.T) {
	v, _ := startFakeVolc(t, func(c *websocket.Conn) { time.Sleep(2 * time.Second) })
	v.Timeout = 200 * time.Millisecond
	start := time.Now()
	if _, _, err := v.Synthesize(context.Background(), "x", TTSSynthesisOpts{}); err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > time.Second {
		t.Errorf("timeout took %v, want ~200ms", time.Since(start))
	}
}

func TestParseVolcengineFrame_Rejects(t *testing.T) {
	if _, err := parseVolcengineFrame([]byte{0x11}); err == nil {
		t.Error("short frame must error")
	}
	trunc := volcServerFrame(volcMsgAudioOnly, 352, []byte("hello"))
	if _, err := parseVolcengineFrame(trunc[:len(trunc)-2]); err == nil {
		t.Error("truncated payload must error")
	}
}

// TestVolcengineTTS_Live hits the real service; it only runs when
// VOLCENGINE_TTS_APPID and VOLCENGINE_TTS_TOKEN are set.
func TestVolcengineTTS_Live(t *testing.T) {
	appID, token := os.Getenv("VOLCENGINE_TTS_APPID"), os.Getenv("VOLCENGINE_TTS_TOKEN")
	if appID == "" || token == "" {
		t.Skip("VOLCENGINE_TTS_APPID / VOLCENGINE_TTS_TOKEN not set")
	}
	v := NewVolcengineTTS(appID, token, os.Getenv("VOLCENGINE_TTS_RESOURCE"), "")
	audio, format, err := v.Synthesize(context.Background(), "测试一下。", TTSSynthesisOpts{Voice: os.Getenv("VOLCENGINE_TTS_VOICE")})
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	if format != "mp3" || len(audio) < 1000 {
		t.Errorf("format = %q, %d bytes; want mp3 with real audio", format, len(audio))
	}
	if out := os.Getenv("VOLCENGINE_TTS_OUT"); out != "" {
		_ = os.WriteFile(out, audio, 0o600)
	}
	t.Logf("got %d bytes of %s", len(audio), format)
}
