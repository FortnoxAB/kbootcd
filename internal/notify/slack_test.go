package notify

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func serve(t *testing.T, h http.HandlerFunc) {
	srv := httptest.NewServer(h)
	old := APIURL
	APIURL = srv.URL
	t.Cleanup(func() {
		APIURL = old
		srv.Close()
	})
}

func TestSlack(t *testing.T) {
	var gotMethod, gotAuth, gotType, gotBody string
	serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	err := Slack(context.Background(), "xoxb-test", "#kbootcd", "hi")
	assert.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "Bearer xoxb-test", gotAuth)
	assert.Equal(t, "application/json; charset=utf-8", gotType)
	assert.Equal(t, `{"channel":"#kbootcd","text":"hi","username":"kbootcd"}`, gotBody)
}

func TestSlackNon2xx(t *testing.T) {
	serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	err := Slack(context.Background(), "xoxb-test", "#kbootcd", "hi")
	assert.ErrorContains(t, err, "500")
}

func TestSlackAPIError(t *testing.T) {
	serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	})

	err := Slack(context.Background(), "xoxb-test", "#kbootcd", "hi")
	assert.ErrorContains(t, err, "channel_not_found")
}
