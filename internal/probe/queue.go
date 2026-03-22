package probe

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	apiBaseURL          = "https://chatgpt.com/backend-api"
	codexClientVersion  = "0.101.0"
	codexUserAgent      = "codex_cli_rs/0.101.0 (Mac OS 26.0.1; arm64) Apple_Terminal/464"
	defaultWaitTailSecs = 5.0
)

type Result struct {
	Status     string `json:"status"`
	HTTPStatus *int   `json:"http_status,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func (r Result) IsBanned() bool {
	return strings.TrimSpace(r.Status) == "banned_401"
}

type Task struct {
	TokenContent map[string]any
	ResponseCh   chan Result
}

type Queue struct {
	delay   time.Duration
	timeout time.Duration

	mu            sync.Mutex
	started       bool
	stopped       bool
	lastProbeTime time.Time
	tasks         chan Task
	stopCh        chan struct{}
	client        *http.Client
}

func New(delaySec float64, timeoutSec float64) *Queue {
	delay := time.Duration(maxFloat(0, delaySec) * float64(time.Second))
	timeout := time.Duration(maxFloat(1, timeoutSec) * float64(time.Second))
	return &Queue{
		delay:   delay,
		timeout: timeout,
		tasks:   make(chan Task, 256),
		stopCh:  make(chan struct{}),
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (q *Queue) Start() {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.mu.Unlock()

	go q.workerLoop()
}

func (q *Queue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	close(q.stopCh)
	q.mu.Unlock()
}

func (q *Queue) Submit(tokenContent map[string]any, waitTimeoutSec float64) Result {
	q.Start()

	task := Task{
		TokenContent: tokenContent,
		ResponseCh:   make(chan Result, 1),
	}

	select {
	case q.tasks <- task:
	case <-q.stopCh:
		return Result{Status: "non_401_error", Detail: "probe_queue_stopped"}
	}

	timeout := q.timeout + q.delay + time.Duration(defaultWaitTailSecs*float64(time.Second))
	if waitTimeoutSec > 0 {
		candidate := time.Duration(waitTimeoutSec * float64(time.Second))
		if candidate > timeout {
			timeout = candidate
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-task.ResponseCh:
		return result
	case <-timer.C:
		return Result{Status: "non_401_error", Detail: "probe_queue_timeout"}
	case <-q.stopCh:
		return Result{Status: "non_401_error", Detail: "probe_queue_stopped"}
	}
}

func (q *Queue) workerLoop() {
	for {
		select {
		case <-q.stopCh:
			return
		case task := <-q.tasks:
			q.applyDelay()
			result := q.probeToken(task.TokenContent)
			q.mu.Lock()
			q.lastProbeTime = time.Now()
			q.mu.Unlock()
			select {
			case task.ResponseCh <- result:
			default:
			}
		}
	}
}

func (q *Queue) applyDelay() {
	q.mu.Lock()
	last := q.lastProbeTime
	delay := q.delay
	q.mu.Unlock()

	if delay <= 0 || last.IsZero() {
		return
	}
	waitFor := delay - time.Since(last)
	if waitFor > 0 {
		timer := time.NewTimer(waitFor)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-q.stopCh:
		}
	}
}

func (q *Queue) probeToken(tokenContent map[string]any) Result {
	accessToken, accountID := extractCredentials(tokenContent)
	if accessToken == "" {
		return Result{Status: "non_401_error", Detail: "missing_access_token"}
	}

	request, err := http.NewRequest(http.MethodGet, apiBaseURL+"/wham/usage", nil)
	if err != nil {
		return Result{Status: "non_401_error", Detail: err.Error()}
	}
	applyCodexHeaders(request, accessToken, accountID)

	response, err := q.client.Do(request)
	if err != nil {
		return Result{Status: "non_401_error", Detail: err.Error()}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return Result{Status: "non_401_error", Detail: err.Error()}
	}

	if response.StatusCode == http.StatusUnauthorized {
		statusCode := response.StatusCode
		return Result{
			Status:     "banned_401",
			HTTPStatus: &statusCode,
			Detail:     strings.TrimSpace(string(body)),
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		statusCode := response.StatusCode
		return Result{
			Status:     "non_401_error",
			HTTPStatus: &statusCode,
			Detail:     strings.TrimSpace(string(body)),
		}
	}

	if !strings.Contains(string(body), "\"plan_type\"") {
		statusCode := response.StatusCode
		return Result{
			Status:     "non_401_error",
			HTTPStatus: &statusCode,
			Detail:     "usage_payload_missing_plan_type",
		}
	}
	return Result{Status: "ok"}
}

func extractStorage(tokenContent map[string]any) map[string]any {
	if storage, ok := tokenContent["storage"].(map[string]any); ok {
		return storage
	}
	return tokenContent
}

func extractCredentials(tokenContent map[string]any) (string, string) {
	storage := extractStorage(tokenContent)
	return strings.TrimSpace(fmt.Sprint(storage["access_token"])), strings.TrimSpace(fmt.Sprint(storage["account_id"]))
}

func applyCodexHeaders(request *http.Request, accessToken string, accountID string) {
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set("Version", codexClientVersion)
	request.Header.Set("Session_id", time.Now().UTC().Format("20060102150405.000000000"))
	request.Header.Set("User-Agent", codexUserAgent)
	request.Header.Set("Accept", "*/*")
	request.Header.Set("Connection", "Keep-Alive")
	request.Header.Set("Accept-Language", "*")
	request.Header.Set("Originator", "codex_vscode")
	if strings.TrimSpace(accountID) != "" {
		request.Header.Set("Chatgpt-Account-Id", strings.TrimSpace(accountID))
	}
}

func DecodeContent(raw string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func maxFloat(left float64, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
