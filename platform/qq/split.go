package qq

import "strings"

// splitReply breaks text into chunks of at most max runes, preferring
// paragraph boundaries, then sentence enders, then a hard cut. max <= 0
// returns the text unchanged.
func splitReply(text string, max int) []string {
	text = strings.TrimSpace(text)
	if max <= 0 || len([]rune(text)) <= max {
		return []string{text}
	}

	var chunks []string
	var cur []rune
	flush := func() {
		if s := strings.TrimSpace(string(cur)); s != "" {
			chunks = append(chunks, s)
		}
		cur = cur[:0]
	}

	for _, para := range strings.Split(text, "\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		for _, piece := range splitSentences(para, max) {
			pr := []rune(piece)
			if len(cur) > 0 && len(cur)+1+len(pr) > max {
				flush()
			}
			if len(cur) > 0 {
				cur = append(cur, '\n')
			}
			cur = append(cur, pr...)
		}
	}
	flush()
	return chunks
}

// splitSentences cuts a single line into pieces of at most max runes at
// sentence enders; a sentence longer than max is cut hard.
func splitSentences(s string, max int) []string {
	var out []string
	var cur []rune
	for _, r := range s {
		cur = append(cur, r)
		if strings.ContainsRune("。！？!?；;", r) && len(cur) >= max/3 || len(cur) >= max {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}
