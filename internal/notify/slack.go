package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var APIURL = "https://slack.com/api/chat.postMessage"

var client = &http.Client{Timeout: 10 * time.Second}

func Slack(ctx context.Context, token, channel, text string) error {
	body, err := json.Marshal(struct {
		Channel  string `json:"channel"`
		Text     string `json:"text"`
		Username string `json:"username"`
	}{channel, text, "kbootcd"})
	if err != nil {
		return fmt.Errorf("marshal msg: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, APIURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("exec request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("slack api returned %d", resp.StatusCode)
	}

	// Slack returns 200 for API errors and signals them in the body
	var res struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if !res.OK {
		return fmt.Errorf("slack api error: %s", res.Error)
	}
	return nil
}
