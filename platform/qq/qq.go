package qq

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func init() {
	core.RegisterPlatform("qq", New)
}

// Platform connects to a OneBot v11 implementation (NapCat, LLOneBot, etc.)
// via forward WebSocket. It receives message events and sends messages back
// through the same WS connection.
type Platform struct {
	wsURL                 string // e.g. "ws://127.0.0.1:3001"
	token                 string // optional access_token
	allowFrom             string // comma-separated user IDs or "*"
	shareSessionInChannel bool
	handler               core.MessageHandler
	conn                  *websocket.Conn
	mu                    sync.Mutex
	echoSeq               atomic.Int64
	echoCh                sync.Map // echo -> chan json.RawMessage
	cancel                context.CancelFunc
	selfID                int64
	dedup                 core.MessageDedup
	groupNameCache        sync.Map // groupID -> group name
	groupNameRefreshing   sync.Map // groupID -> in-flight async lookup guard
	httpURL               string   // OneBot HTTP API URL, e.g. "http://127.0.0.1:3000"
	ackEmojiID            int64    // emoji reaction added when a message is accepted (0 = disabled)
	doneEmojiID           int64    // emoji reaction added when the agent finishes (0 = disabled)
	queues                sync.Map // chat key -> *chatQueue (ordered outbound queue)
	sendIntervalMin       time.Duration
	sendIntervalMax       time.Duration
	finalMaxChars         int // final replies longer than this are split into several messages (0 = off)
}

func New(opts map[string]any) (core.Platform, error) {
	wsURL, _ := opts["ws_url"].(string)
	if wsURL == "" {
		wsURL = "ws://127.0.0.1:3001"
	}
	token, _ := opts["token"].(string)
	allowFrom, _ := opts["allow_from"].(string)
	shareSessionInChannel, _ := opts["share_session_in_channel"].(bool)

	core.CheckAllowFrom("qq", allowFrom)

	httpURL, _ := opts["http_url"].(string)
	httpURL = strings.TrimRight(httpURL, "/")

	return &Platform{
		wsURL:                 wsURL,
		token:                 token,
		allowFrom:             allowFrom,
		shareSessionInChannel: shareSessionInChannel,
		httpURL:               httpURL,
		ackEmojiID:            optInt64(opts, "ack_emoji_id"),
		doneEmojiID:           optInt64(opts, "done_emoji_id"),
		sendIntervalMin:       time.Duration(optInt64Default(opts, "send_interval_min_ms", defaultSendIntervalMinMS)) * time.Millisecond,
		sendIntervalMax:       time.Duration(optInt64Default(opts, "send_interval_max_ms", defaultSendIntervalMaxMS)) * time.Millisecond,
		finalMaxChars:         int(optInt64Default(opts, "final_reply_max_chars", defaultFinalReplyMaxChars)),
	}, nil
}

// optInt64Default is optInt64 with a default for absent keys, so an explicit 0
// can still disable a feature.
func optInt64Default(opts map[string]any, key string, def int64) int64 {
	if _, ok := opts[key]; !ok {
		return def
	}
	return optInt64(opts, key)
}

// optInt64 reads an integer option. TOML decodes integers as int64, but JSON
// overrides and hand-written configs may surface float64 or numeric strings.
func optInt64(opts map[string]any, key string) int64 {
	switch v := opts[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	}
	return 0
}

func (p *Platform) Name() string { return "qq" }

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler

	header := http.Header{}
	if p.token != "" {
		header.Set("Authorization", "Bearer "+p.token)
	}

	conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
	if err != nil {
		return fmt.Errorf("qq: ws connect failed (%s): %w", p.wsURL, err)
	}
	p.conn = conn

	slog.Info("qq: connected to OneBot", "url", p.wsURL)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	// Start readLoop BEFORE callAPI: callAPI's response is routed by readLoop,
	// so calling it first would always time out after 15s and leave selfID=0,
	// which disables the self-message filter in handleMessage and lets the bot
	// respond to its own messages.
	go p.readLoop(ctx)

	// Get bot self info
	if info, err := p.callAPI("get_login_info", nil); err == nil {
		if uid, ok := info["user_id"].(float64); ok {
			p.selfID = int64(uid)
		}
		nick, _ := info["nickname"].(string)
		slog.Info("qq: logged in", "qq", p.selfID, "nickname", nick)
	} else {
		slog.Warn("qq: get_login_info failed; self-message filter disabled until next reconnect", "error", err)
	}

	return nil
}

func (p *Platform) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("qq: ws read error, reconnecting...", "error", err)
			p.reconnect()
			continue
		}

		var payload map[string]any
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}

		// If this is an API response (has "echo" field), route to caller
		if echo, ok := payload["echo"].(string); ok {
			if ch, loaded := p.echoCh.LoadAndDelete(echo); loaded {
				if dataCh, ok := ch.(chan json.RawMessage); ok {
					dataCh <- raw
				}
			}
			continue
		}

		// Otherwise it's an event
		postType, _ := payload["post_type"].(string)
		if postType == "message" {
			p.handleMessage(payload)
		}
	}
}

func (p *Platform) reconnect() {
	for i := 1; i <= 30; i++ {
		time.Sleep(time.Duration(i) * 2 * time.Second)
		header := http.Header{}
		if p.token != "" {
			header.Set("Authorization", "Bearer "+p.token)
		}
		conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
		if err != nil {
			slog.Warn("qq: reconnect attempt failed", "attempt", i, "error", err)
			continue
		}
		p.mu.Lock()
		p.conn = conn
		p.mu.Unlock()
		slog.Info("qq: reconnected")
		return
	}
	slog.Error("qq: failed to reconnect after 30 attempts")
}

func (p *Platform) handleMessage(payload map[string]any) {
	msgType, _ := payload["message_type"].(string)
	userID := jsonInt64(payload, "user_id")
	groupID := jsonInt64(payload, "group_id")
	messageID := jsonInt64(payload, "message_id")

	if userID == p.selfID {
		return
	}

	// Group messages require an explicit @bot mention; private chats pass freely.
	// When selfID is unknown (get_login_info failed), keep the legacy behavior
	// and process all group messages rather than bricking the bot.
	if msgType == "group" && p.selfID != 0 && !p.isAtBot(payload) {
		slog.Debug("qq: group message without @bot ignored", "user", userID, "group", groupID)
		return
	}

	if ts, ok := payload["time"].(float64); ok && ts > 0 {
		if core.IsOldMessage(time.Unix(int64(ts), 0)) {
			slog.Debug("qq: ignoring old message after restart", "time", int64(ts))
			return
		}
	}

	msgIDStr := strconv.FormatInt(messageID, 10)
	if p.dedup.IsDuplicate(msgIDStr) {
		slog.Debug("qq: duplicate message ignored", "message_id", messageID)
		return
	}

	if !p.isAllowed(userID) {
		return
	}

	// Extract sender info
	var userName string
	if sender, ok := payload["sender"].(map[string]any); ok {
		card, _ := sender["card"].(string)
		nick, _ := sender["nickname"].(string)
		if card != "" {
			userName = card
		} else {
			userName = nick
		}
	}

	// Parse message content from CQ message array or raw_message
	text, images, files, audio := p.parseMessage(payload, msgType, groupID)
	if text == "" && len(images) == 0 && len(files) == 0 && audio == nil {
		return
	}

	var sessionKey string
	if msgType == "group" {
		if p.shareSessionInChannel {
			sessionKey = fmt.Sprintf("qq:g:%d", groupID)
		} else {
			sessionKey = fmt.Sprintf("qq:%d:%d", groupID, userID)
		}
	} else {
		sessionKey = fmt.Sprintf("qq:%d", userID)
	}

	rctx := &replyContext{
		messageType: msgType,
		userID:      userID,
		groupID:     groupID,
		messageID:   int32(messageID),
	}

	var chatName string
	if msgType == "group" {
		chatName = p.resolveGroupName(groupID)
	}

	msg := &core.Message{
		SessionKey: sessionKey,
		Platform:   "qq",
		MessageID:  strconv.FormatInt(messageID, 10),
		UserID:     strconv.FormatInt(userID, 10),
		UserName:   userName,
		ChatName:   chatName,
		Content:    text,
		Images:     images,
		Files:      files,
		Audio:      audio,
		ReplyCtx:   rctx,
	}
	p.prepareReceiptAcknowledgement(msg, rctx)

	slog.Debug("qq: message received", "type", msgType, "user", userID, "text_len", len(text))
	p.handler(p, msg)
}

func (p *Platform) parseMessage(payload map[string]any, msgType string, groupID int64) (string, []core.ImageAttachment, []core.FileAttachment, *core.AudioAttachment) {
	var textParts []string
	var images []core.ImageAttachment
	var files []core.FileAttachment
	var audio *core.AudioAttachment

	// OneBot message can be array of segments or a string
	switch msg := payload["message"].(type) {
	case []any:
		for _, seg := range msg {
			s, ok := seg.(map[string]any)
			if !ok {
				continue
			}
			segType, _ := s["type"].(string)
			data, _ := s["data"].(map[string]any)
			if data == nil {
				continue
			}

			switch segType {
			case "text":
				if text, ok := data["text"].(string); ok {
					textParts = append(textParts, text)
				}
			case "image":
				if url, ok := data["url"].(string); ok && url != "" {
					imgData, mime, err := downloadFile(url)
					if err != nil {
						slog.Warn("qq: download image failed", "error", err)
						continue
					}
					images = append(images, core.ImageAttachment{
						MimeType: mime,
						Data:     imgData,
					})
				}
			case "record":
				if url, ok := data["url"].(string); ok && url != "" {
					audioData, _, err := downloadFile(url)
					if err != nil {
						slog.Warn("qq: download audio failed", "error", err)
						continue
					}
					format := "silk"
					if f, ok := data["file"].(string); ok {
						if strings.HasSuffix(f, ".amr") {
							format = "amr"
						} else if strings.HasSuffix(f, ".mp3") {
							format = "mp3"
						}
					}
					audio = &core.AudioAttachment{
						Data:   audioData,
						Format: format,
					}
				}
			case "file":
				name, _ := data["name"].(string)
				if name == "" {
					name, _ = data["file"].(string)
				}
				fileID, _ := data["file_id"].(string)
				if fileID == "" {
					fileID, _ = data["file"].(string)
				}

				slog.Info("qq: file segment received", "name", name, "file_id", fileID,
					"has_url", data["url"] != nil, "msg_type", msgType, "group_id", groupID)

				var downloaded bool

				// Step 1: Try direct URL from message segment (with longer timeout for large files)
				if url, ok := data["url"].(string); ok && url != "" {
					fileData, mime, err := downloadLargeFile(url)
					if err != nil {
						slog.Warn("qq: [step1] download file via segment URL failed", "error", err)
					} else {
						files = append(files, core.FileAttachment{
							MimeType: mime,
							Data:     fileData,
							FileName: name,
						})
						downloaded = true
						slog.Info("qq: [step1] file downloaded via segment URL", "name", name, "size", len(fileData))
					}
				}

				// Step 2: Get fresh direct link via NapCat API (CDN URLs expire / have download limits)
				// NapCat docs: params are STRINGS — group (not group_id), file_id
				if !downloaded && p.httpURL != "" && fileID != "" {
					var freshURL string
					if msgType == "group" && groupID != 0 {
						groupStr := strconv.FormatInt(groupID, 10)
						slog.Info("qq: [step2] trying get_group_file_url", "file_id", fileID, "group", groupStr)
						result, err := p.callHTTPAPI("get_group_file_url", map[string]any{
							"file_id": fileID,
							"group":   groupStr,
						})
						if err == nil {
							freshURL, _ = result["url"].(string)
							slog.Info("qq: [step2] get_group_file_url returned", "has_url", freshURL != "")
						} else {
							slog.Warn("qq: [step2] get_group_file_url failed", "file_id", fileID, "error", err)
						}
					} else {
						slog.Info("qq: [step2] trying get_private_file_url", "file_id", fileID)
						result, err := p.callHTTPAPI("get_private_file_url", map[string]any{
							"file_id": fileID,
						})
						if err == nil {
							freshURL, _ = result["url"].(string)
						} else {
							slog.Warn("qq: [step2] get_private_file_url failed", "file_id", fileID, "error", err)
						}
					}
					if freshURL != "" {
						fileData, mime, err := downloadLargeFile(freshURL)
						if err == nil {
							files = append(files, core.FileAttachment{
								MimeType: mime,
								Data:     fileData,
								FileName: name,
							})
							downloaded = true
							slog.Info("qq: [step2] file downloaded via fresh URL", "name", name, "size", len(fileData))
						} else {
							slog.Warn("qq: [step2] download file via fresh URL failed", "error", err)
						}
					}
				}

				// Step 3: Last resort — get_file (downloads to NapCat local or returns base64)
				if !downloaded && p.httpURL != "" && fileID != "" {
					slog.Info("qq: [step3] trying get_file", "file_id", fileID)
					result, err := p.callHTTPAPI("get_file", map[string]any{"file_id": fileID})
					if err == nil {
						if fileURL, ok := result["url"].(string); ok && fileURL != "" {
							fileData, mime, err := downloadLargeFile(fileURL)
							if err == nil {
								files = append(files, core.FileAttachment{
									MimeType: mime,
									Data:     fileData,
									FileName: name,
								})
								downloaded = true
							}
						}
						if !downloaded {
							if b64Str, ok := result["base64"].(string); ok && b64Str != "" {
								if decoded, err := base64.StdEncoding.DecodeString(b64Str); err == nil {
									files = append(files, core.FileAttachment{
										MimeType: http.DetectContentType(decoded),
										Data:     decoded,
										FileName: name,
									})
									downloaded = true
								}
							}
						}
					} else {
						slog.Warn("qq: get_file API failed", "file_id", fileID, "error", err)
					}
				}

				if !downloaded {
					slog.Warn("qq: file segment could not be downloaded", "name", name)
				}
			case "at":
				// Ignore @mentions in parsed text
			}
		}
	default:
		// raw_message fallback (string with CQ codes)
		if raw, ok := payload["raw_message"].(string); ok {
			textParts = append(textParts, stripCQCodes(raw))
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "")), images, files, audio
}

// Reply sends a message as a reply to an incoming message.
func (p *Platform) Reply(ctx context.Context, replyCtx any, content string) error {
	return p.Send(ctx, replyCtx, content)
}

// Send sends a message to the conversation identified by replyCtx. It goes
// through the chat's ordered queue and returns after the message was sent.
func (p *Platform) Send(ctx context.Context, replyCtx any, content string) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: invalid reply context")
	}
	return p.queueFor(chatKey(rctx)).submit(ctx, kindMessage, func() error {
		_, err := p.callAPI(sendAction(rctx), sendParams(rctx, content))
		return err
	})
}

// sendAction and sendParams build a plain send_*_msg call for the chat.
func sendAction(rctx *replyContext) string {
	if rctx.messageType == "group" {
		return "send_group_msg"
	}
	return "send_private_msg"
}

func sendParams(rctx *replyContext, message any) map[string]any {
	params := map[string]any{"message": message}
	if rctx.messageType == "group" {
		params["group_id"] = rctx.groupID
	} else {
		params["user_id"] = rctx.userID
	}
	return params
}

// SendFinalReply sends the agent's final answer of a turn. In group chats the
// first message quotes the triggering message with a reply segment; side
// messages sent via Send (e.g. `cc-connect send`) stay unquoted. Answers longer
// than final_reply_max_chars are split into several messages, spaced like any
// other consecutive messages, as a safety net against walls of text.
// Implements core.FinalReplySender.
func (p *Platform) SendFinalReply(ctx context.Context, replyCtx any, content string) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: invalid reply context")
	}
	chunks := splitReply(content, p.finalMaxChars)
	quote := rctx.messageType == "group" && rctx.messageID != 0

	return p.queueFor(chatKey(rctx)).submit(ctx, kindMessage, func() error {
		for i, chunk := range chunks {
			if i > 0 {
				time.Sleep(p.randomGap())
			}
			var message any = chunk
			if i == 0 && quote {
				message = []map[string]any{
					{"type": "reply", "data": map[string]any{"id": strconv.FormatInt(int64(rctx.messageID), 10)}},
					{"type": "text", "data": map[string]any{"text": chunk}},
				}
			}
			if _, err := p.callAPI(sendAction(rctx), sendParams(rctx, message)); err != nil {
				return err
			}
		}
		return nil
	})
}

// SendImage sends an image to the conversation.
// Implements core.ImageSender.
func (p *Platform) SendImage(ctx context.Context, replyCtx any, img core.ImageAttachment) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: SendImage: invalid reply context type %T", replyCtx)
	}

	b64 := base64.StdEncoding.EncodeToString(img.Data)
	segments := []map[string]any{
		{"type": "image", "data": map[string]any{"file": "base64://" + b64}},
	}

	return p.queueFor(chatKey(rctx)).submit(ctx, kindMessage, func() error {
		if _, err := p.callAPI(sendAction(rctx), sendParams(rctx, segments)); err != nil {
			return fmt.Errorf("qq: send image: %w", err)
		}
		return nil
	})
}

var _ core.ImageSender = (*Platform)(nil)

// SendAudio sends an audio clip as a native QQ voice message (OneBot "record"
// segment). NapCat transcodes common formats (mp3, wav, ogg, ...) to QQ's
// voice codec itself, so the bytes are passed through; format is only a hint.
// Implements core.AudioSender.
func (p *Platform) SendAudio(ctx context.Context, replyCtx any, audio []byte, format string) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: SendAudio: invalid reply context type %T", replyCtx)
	}
	if len(audio) == 0 {
		return fmt.Errorf("qq: SendAudio: empty audio")
	}
	segments := []map[string]any{
		{"type": "record", "data": map[string]any{"file": "base64://" + base64.StdEncoding.EncodeToString(audio)}},
	}
	return p.queueFor(chatKey(rctx)).submit(ctx, kindMessage, func() error {
		if _, err := p.callAPI(sendAction(rctx), sendParams(rctx, segments)); err != nil {
			return fmt.Errorf("qq: send voice (%s): %w", format, err)
		}
		return nil
	})
}

var _ core.AudioSender = (*Platform)(nil)

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// isAtBot reports whether the message contains an at-segment targeting the bot
// itself. Per the OneBot v11 event spec the mention arrives as
// {"type":"at","data":{"qq":"<bot qq>"}}; NapCat sends qq as a string, other
// implementations may use a number. For CQ-code string messages the check
// falls back to raw_message.
func (p *Platform) isAtBot(payload map[string]any) bool {
	self := strconv.FormatInt(p.selfID, 10)

	switch msg := payload["message"].(type) {
	case []any:
		for _, seg := range msg {
			s, ok := seg.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := s["type"].(string); t != "at" {
				continue
			}
			data, _ := s["data"].(map[string]any)
			if data == nil {
				continue
			}
			switch qq := data["qq"].(type) {
			case string:
				if qq == self {
					return true
				}
			case float64:
				if int64(qq) == p.selfID {
					return true
				}
			case json.Number:
				if n, err := qq.Int64(); err == nil && n == p.selfID {
					return true
				}
			}
		}
		return false
	default:
		// CQ-code string fallback: [CQ:at,qq=12345] / [CQ:at,qq=12345,...].
		// The trailing , or ] guards against prefix collisions between QQ IDs.
		raw, _ := payload["message"].(string)
		if raw == "" {
			raw, _ = payload["raw_message"].(string)
		}
		code := "[CQ:at,qq=" + self
		return raw != "" && (strings.Contains(raw, code+",") || strings.Contains(raw, code+"]"))
	}
}

func (p *Platform) resolveGroupName(groupID int64) string {
	if groupID == 0 {
		return ""
	}
	fallback := strconv.FormatInt(groupID, 10)
	if cached, ok := p.groupNameCache.Load(fallback); ok {
		return cached.(string)
	}
	// The get_group_info lookup MUST NOT run inline: handleMessage executes on
	// the readLoop goroutine, and callAPI's echo response is routed by that
	// same readLoop — an inline call would deadlock every incoming group
	// message for the full 15s API timeout. Refresh asynchronously on cache
	// miss; this message falls back to the numeric group ID.
	if _, refreshing := p.groupNameRefreshing.LoadOrStore(fallback, struct{}{}); !refreshing {
		go func() {
			defer p.groupNameRefreshing.Delete(fallback)
			result, err := p.callAPI("get_group_info", map[string]any{"group_id": groupID})
			if err != nil {
				slog.Debug("qq: resolve group name failed", "group_id", groupID, "error", err)
				return
			}
			if name, _ := result["group_name"].(string); name != "" {
				p.groupNameCache.Store(fallback, name)
			}
		}()
	}
	return fallback
}

// ── OneBot API call via WebSocket ───────────────────────────────

func (p *Platform) callAPI(action string, params map[string]any) (map[string]any, error) {
	seq := p.echoSeq.Add(1)
	echo := strconv.FormatInt(seq, 10)

	req := map[string]any{
		"action": action,
		"echo":   echo,
	}
	if params != nil {
		req["params"] = params
	}

	ch := make(chan json.RawMessage, 1)
	p.echoCh.Store(echo, ch)
	defer p.echoCh.Delete(echo)

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.conn == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("qq: ws connection not established")
	}
	err = p.conn.WriteMessage(websocket.TextMessage, data)
	p.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("qq: ws write: %w", err)
	}

	select {
	case raw := <-ch:
		var resp struct {
			Status  string          `json:"status"`
			RetCode int             `json:"retcode"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &resp) != nil {
			return nil, fmt.Errorf("qq: invalid API response")
		}
		if resp.RetCode != 0 {
			return nil, fmt.Errorf("qq: API %s failed (retcode=%d)", action, resp.RetCode)
		}
		var result map[string]any
		_ = json.Unmarshal(resp.Data, &result)
		return result, nil

	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("qq: API %s timeout", action)
	}
}

// callHTTPAPI calls a OneBot v11 HTTP endpoint (e.g. /upload_group_file).
// Used for file operations — avoids WebSocket message size limits and
// file-path issues across Windows/WSL/Docker boundaries.
// Requires http_url to be configured.
func (p *Platform) callHTTPAPI(action string, params map[string]any) (map[string]any, error) {
	if p.httpURL == "" {
		return nil, fmt.Errorf("qq: http_url not configured")
	}
	body, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	url := p.httpURL + "/" + action
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("qq: HTTP %s failed: %w", action, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("qq: HTTP %s read body: %w", action, err)
	}

	var apiResp struct {
		Status  string          `json:"status"`
		RetCode int             `json:"retcode"`
		Data    json.RawMessage `json:"data"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(raw, &apiResp) != nil {
		return nil, fmt.Errorf("qq: HTTP %s invalid response", action)
	}
	if apiResp.RetCode != 0 {
		return nil, fmt.Errorf("qq: HTTP %s failed (retcode=%d, msg=%s)", action, apiResp.RetCode, apiResp.Message)
	}
	var result map[string]any
	_ = json.Unmarshal(apiResp.Data, &result)
	return result, nil
}

// ── Emoji reactions (NapCat set_msg_emoji_like extension) ────────

// prepareReceiptAcknowledgement wires msg.OnAccepted so the engine's
// acceptance hook — fired when the message starts processing or is queued —
// adds the configured ack emoji to the user's message. Mirrors the feishu
// adapter's receipt semantics; disabled when ack_emoji_id is not configured.
func (p *Platform) prepareReceiptAcknowledgement(msg *core.Message, rctx *replyContext) {
	if p.ackEmojiID == 0 || rctx.messageID == 0 {
		return
	}
	messageID := int64(rctx.messageID)
	key := chatKey(rctx)
	msg.OnAccepted = func() {
		go func() {
			err := p.queueFor(key).submit(context.Background(), kindAction, func() error {
				return p.setMsgEmojiLike(messageID, p.ackEmojiID, true)
			})
			if err != nil {
				slog.Warn("qq: ack reaction failed", "message_id", messageID, "error", err)
			}
		}()
	}
}

// AddDoneReaction implements core.TypingIndicatorDone: the engine calls it
// after the agent's final answer to add the configured done emoji to the
// triggering user message. Disabled when done_emoji_id is not configured.
func (p *Platform) AddDoneReaction(rctx any) {
	if p.doneEmojiID == 0 {
		return
	}
	rc, ok := rctx.(*replyContext)
	if !ok || rc.messageID == 0 {
		return
	}
	messageID := int64(rc.messageID)
	key := chatKey(rc)
	go func() {
		err := p.queueFor(key).submit(context.Background(), kindAction, func() error {
			return p.setMsgEmojiLike(messageID, p.doneEmojiID, true)
		})
		if err != nil {
			slog.Warn("qq: done reaction failed", "message_id", messageID, "error", err)
		}
	}()
}

// setMsgEmojiLike calls NapCat's set_msg_emoji_like action to add (set=true)
// or remove (set=false) an emoji reaction on a message. This action is a
// NapCat extension beyond the OneBot v11 standard; implementations without
// it return an error, which callers log and ignore.
func (p *Platform) setMsgEmojiLike(messageID, emojiID int64, set bool) error {
	_, err := p.callAPI("set_msg_emoji_like", map[string]any{
		"message_id": messageID,
		"emoji_id":   emojiID,
		"set":        set,
	})
	return err
}

var _ core.TypingIndicatorDone = (*Platform)(nil)

// ── Platform actions (poke, typing status, ...) ──────────────────

// queuedActions are the non-message NapCat actions an agent may trigger via
// `cc-connect send --action`. They are small, reversible and safe to run from
// a chat; anything riskier (ban, kick, recall, profile changes) stays out and
// needs the user's explicit approval through other channels.
var queuedActions = map[string]bool{
	"group_poke":         true,
	"friend_poke":        true,
	"send_poke":          true,
	"set_input_status":   true,
	"set_msg_emoji_like": true,
	"set_group_sign":     true,
}

// DoAction runs one whitelisted NapCat action inside the chat's ordered queue,
// so it executes after every message queued before it. Missing group_id /
// user_id params are filled in from the conversation.
// Implements core.PlatformActionSender.
func (p *Platform) DoAction(ctx context.Context, replyCtx any, action string, params map[string]any) (map[string]any, error) {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return nil, fmt.Errorf("qq: invalid reply context")
	}
	if !queuedActions[action] {
		return nil, fmt.Errorf("qq: action %q is not allowed here", action)
	}
	if params == nil {
		params = map[string]any{}
	}
	if rctx.messageType == "group" {
		if _, ok := params["group_id"]; !ok && action != "friend_poke" && action != "set_msg_emoji_like" {
			params["group_id"] = rctx.groupID
		}
	} else if _, ok := params["user_id"]; !ok && action != "set_msg_emoji_like" {
		params["user_id"] = rctx.userID
	}

	var result map[string]any
	err := p.queueFor(chatKey(rctx)).submit(ctx, kindAction, func() error {
		var err error
		result, err = p.callAPI(action, params)
		return err
	})
	return result, err
}

var _ core.PlatformActionSender = (*Platform)(nil)

// ── Helpers ─────────────────────────────────────────────────────

type replyContext struct {
	messageType string // "private" or "group"
	userID      int64
	groupID     int64
	messageID   int32
}

func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	// qq:{userID}, qq:{groupID}:{userID} or qq:g:{groupID}
	parts := strings.SplitN(sessionKey, ":", 3)
	if len(parts) < 2 || parts[0] != "qq" {
		return nil, fmt.Errorf("qq: invalid session key %q", sessionKey)
	}
	if len(parts) == 3 {
		if parts[1] == "g" {
			gid, _ := strconv.ParseInt(parts[2], 10, 64)
			return &replyContext{messageType: "group", groupID: gid}, nil
		}
		gid, _ := strconv.ParseInt(parts[1], 10, 64)
		uid, _ := strconv.ParseInt(parts[2], 10, 64)
		return &replyContext{messageType: "group", groupID: gid, userID: uid}, nil
	}
	uid, _ := strconv.ParseInt(parts[1], 10, 64)
	return &replyContext{messageType: "private", userID: uid}, nil
}

func (p *Platform) isAllowed(userID int64) bool {
	if p.allowFrom == "" || p.allowFrom == "*" {
		return true
	}
	uid := strconv.FormatInt(userID, 10)
	for _, allowed := range strings.Split(p.allowFrom, ",") {
		if strings.TrimSpace(allowed) == uid {
			return true
		}
	}
	return false
}

func jsonInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

func stripCQCodes(s string) string {
	var result strings.Builder
	for len(s) > 0 {
		idx := strings.Index(s, "[CQ:")
		if idx < 0 {
			result.WriteString(s)
			break
		}
		result.WriteString(s[:idx])
		end := strings.Index(s[idx:], "]")
		if end < 0 {
			break
		}
		s = s[idx+end+1:]
	}
	return result.String()
}

func downloadLargeFile(url string) ([]byte, string, error) {
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	mime := resp.Header.Get("Content-Type")
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	return data, mime, nil
}

func downloadFile(url string) ([]byte, string, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	mime := resp.Header.Get("Content-Type")
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	return data, mime, nil
}

// SendFile sends a file to the conversation.
// Implements core.FileSender.
//
// Uses base64-encoded file data to avoid file-path issues across
// Windows/WSL/Docker. Routes through NapCat HTTP API when configured
// (better for large files), falls back to WebSocket.
func (p *Platform) SendFile(ctx context.Context, replyCtx any, file core.FileAttachment) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: SendFile: invalid reply context type %T", replyCtx)
	}

	name := file.FileName
	if name == "" {
		name = "attachment"
	}

	b64data := "base64://" + base64.StdEncoding.EncodeToString(file.Data)

	// Pick API caller: prefer HTTP for large payloads, fall back to WebSocket.
	call := p.callAPI
	if p.httpURL != "" {
		call = p.callHTTPAPI
	}

	return p.queueFor(chatKey(rctx)).submit(ctx, kindMessage, func() error {
		if rctx.messageType == "group" {
			_, err := call("upload_group_file", map[string]any{
				"group_id": rctx.groupID,
				"file":     b64data,
				"name":     name,
			})
			if err != nil {
				return fmt.Errorf("qq: SendFile group: %w", err)
			}
			return nil
		}

		// Private: use send_private_msg with file segment
		_, err := call("send_private_msg", map[string]any{
			"user_id": rctx.userID,
			"message": []map[string]any{
				{"type": "file", "data": map[string]any{"file": b64data, "name": name}},
			},
		})
		if err != nil {
			return fmt.Errorf("qq: SendFile private: %w", err)
		}
		return nil
	})
}

var _ core.FileSender = (*Platform)(nil)
