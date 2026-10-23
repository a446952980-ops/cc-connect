package core

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ──────────────────────────────────────────────────────────────
// VolcengineTTS — Volcengine (Doubao) speech synthesis, WebSocket
// unidirectional streaming V3
// ──────────────────────────────────────────────────────────────

const (
	volcengineDefaultURL        = "wss://openspeech.bytedance.com/api/v3/tts/unidirectional/stream"
	volcengineDefaultResourceID = "seed-tts-2.0"
	volcengineDefaultSpeaker    = "zh_female_vv_uranus_bigtts"

	volcMsgFullClient = 0x1 // client request
	volcMsgAudioOnly  = 0xB // server frame carrying raw audio
	volcMsgError      = 0xF // server error frame
	volcFlagEvent     = 0x4 // frame carries an event number

	volcEventSessionFinished = 152
	volcEventSessionFailed   = 153
	volcEventConnFailed      = 51
)

// VolcengineTTS implements TextToSpeech using the Volcengine unidirectional
// streaming WebSocket API. One connection serves one synthesis request: the
// client sends a single request frame and receives audio chunks until the
// session-finished event.
type VolcengineTTS struct {
	AppID       string
	AccessToken string
	ResourceID  string
	URL         string
	Timeout     time.Duration
	Dialer      *websocket.Dialer
}

// NewVolcengineTTS creates a VolcengineTTS. Empty resourceID / url use the
// defaults (TTS 2.0 resource, production endpoint).
func NewVolcengineTTS(appID, accessToken, resourceID, url string) *VolcengineTTS {
	if resourceID == "" {
		resourceID = volcengineDefaultResourceID
	}
	if url == "" {
		url = volcengineDefaultURL
	}
	return &VolcengineTTS{
		AppID:       appID,
		AccessToken: accessToken,
		ResourceID:  resourceID,
		URL:         url,
		Timeout:     30 * time.Second,
		Dialer:      websocket.DefaultDialer,
	}
}

// Synthesize streams text to Volcengine and returns the assembled MP3 audio.
func (v *VolcengineTTS) Synthesize(ctx context.Context, text string, opts TTSSynthesisOpts) ([]byte, string, error) {
	speaker := opts.Voice
	if speaker == "" {
		speaker = volcengineDefaultSpeaker
	}
	audioParams := map[string]any{"format": "mp3", "sample_rate": 24000}
	if opts.Speed > 0 && opts.Speed != 1 {
		// Volcengine speech_rate is [-50, 100]: -50 = 0.5x, 0 = 1x, 100 = 2x.
		rate := int((opts.Speed - 1) * 100)
		if rate < -50 {
			rate = -50
		}
		if rate > 100 {
			rate = 100
		}
		audioParams["speech_rate"] = rate
	}
	body, err := json.Marshal(map[string]any{
		"user": map[string]any{"uid": "cc-connect"},
		"req_params": map[string]any{
			"text":         text,
			"speaker":      speaker,
			"audio_params": audioParams,
		},
	})
	if err != nil {
		return nil, "", fmt.Errorf("volcengine tts: marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, v.Timeout)
	defer cancel()

	header := http.Header{}
	header.Set("X-Api-App-Id", v.AppID)
	header.Set("X-Api-Access-Key", v.AccessToken)
	header.Set("X-Api-Resource-Id", v.ResourceID)
	header.Set("X-Api-Connect-Id", newVolcengineConnectID())

	conn, resp, err := v.Dialer.DialContext(ctx, v.URL, header)
	if err != nil {
		if resp != nil {
			msg := resp.Header.Get("X-Api-Message")
			return nil, "", fmt.Errorf("volcengine tts: dial (HTTP %d %s): %w", resp.StatusCode, msg, err)
		}
		return nil, "", fmt.Errorf("volcengine tts: dial: %w", err)
	}
	defer conn.Close()

	// Closing the connection on cancel unblocks the read loop below.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()

	if err := conn.WriteMessage(websocket.BinaryMessage, volcengineRequestFrame(body)); err != nil {
		return nil, "", fmt.Errorf("volcengine tts: send request: %w", err)
	}

	var audio []byte
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil, "", fmt.Errorf("volcengine tts: %w", ctx.Err())
			}
			return nil, "", fmt.Errorf("volcengine tts: read: %w", err)
		}
		f, err := parseVolcengineFrame(frame)
		if err != nil {
			return nil, "", fmt.Errorf("volcengine tts: %w", err)
		}
		switch {
		case f.msgType == volcMsgError:
			return nil, "", fmt.Errorf("volcengine tts: server error %d: %s", f.errCode, truncateForLog(string(f.payload)))
		case f.msgType == volcMsgAudioOnly:
			audio = append(audio, f.payload...)
		case f.hasEvent && (f.event == volcEventSessionFailed || f.event == volcEventConnFailed):
			return nil, "", fmt.Errorf("volcengine tts: session failed: %s", truncateForLog(string(f.payload)))
		case f.hasEvent && f.event == volcEventSessionFinished:
			if len(audio) == 0 {
				return nil, "", fmt.Errorf("volcengine tts: no audio returned")
			}
			return audio, "mp3", nil
		}
	}
}

// volcengineRequestFrame builds the single full-client request frame:
// 4-byte header (v1, header size 1, full client request, JSON, no
// compression) + big-endian payload size + payload.
func volcengineRequestFrame(payload []byte) []byte {
	frame := make([]byte, 0, 8+len(payload))
	frame = append(frame, 0x11, volcMsgFullClient<<4, 0x10, 0x00)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	return append(frame, payload...)
}

type volcengineFrame struct {
	msgType  byte
	flags    byte
	hasEvent bool
	event    int32
	errCode  uint32
	payload  []byte
}

// parseVolcengineFrame decodes a server frame. Layout after the 4-byte
// header: [sequence] [event] [session id (len+bytes)] [error code] then
// payload size + payload; every integer is big-endian.
func parseVolcengineFrame(b []byte) (volcengineFrame, error) {
	var f volcengineFrame
	if len(b) < 4 {
		return f, fmt.Errorf("short frame (%d bytes)", len(b))
	}
	f.msgType, f.flags = b[1]>>4, b[1]&0x0F
	off := int(b[0]&0x0F) * 4
	if off < 4 || off > len(b) {
		return f, fmt.Errorf("bad header size")
	}
	u32 := func() (uint32, bool) {
		if off+4 > len(b) {
			return 0, false
		}
		v := binary.BigEndian.Uint32(b[off:])
		off += 4
		return v, true
	}

	if f.flags == 1 || f.flags == 3 { // sequence number
		if _, ok := u32(); !ok {
			return f, fmt.Errorf("truncated sequence")
		}
	}
	if f.msgType == volcMsgError {
		code, ok := u32()
		if !ok {
			return f, fmt.Errorf("truncated error code")
		}
		f.errCode = code
	} else if f.flags&volcFlagEvent != 0 {
		ev, ok := u32()
		if !ok {
			return f, fmt.Errorf("truncated event")
		}
		f.hasEvent, f.event = true, int32(ev)
		// Connection events 50-52 and session events (>= 100) carry an id.
		if ev == 50 || ev == 51 || ev == 52 || ev >= 100 {
			n, ok := u32()
			if !ok || off+int(n) > len(b) {
				return f, fmt.Errorf("truncated session id")
			}
			off += int(n)
		}
	}
	n, ok := u32()
	if !ok || off+int(n) > len(b) {
		return f, fmt.Errorf("truncated payload")
	}
	f.payload = b[off : off+int(n)]
	return f, nil
}

func newVolcengineConnectID() string {
	return strings.ToLower(fmt.Sprintf("cc-%d", time.Now().UnixNano()))
}

// truncateForLog shortens server messages before they go into errors.
func truncateForLog(s string) string {
	const max = 200
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "…"
	}
	return s
}
