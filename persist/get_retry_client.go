package persist

import (
	"errors"
	"net/http"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/logging"
)

type GetRetryClient struct {
	inner   HTTPClient
	retries int
	sleeper *clock.Sleeper
	logger  *logging.Logger
}

// FUTURE: We may want to consider a ShutdownClient that sits just under
// the RetryClient. This makes it possible for a shutdown signal to break
// a retry loop because the Shutdown client would retry success (HTTP 200)
// or perhaps HTTP 404?

func NewGetRetryClient(inner HTTPClient, retries int) *GetRetryClient {
	return &GetRetryClient{inner: inner, retries: retries}
}

func (this *GetRetryClient) Do(request *http.Request) (*http.Response, error) {
	for current := 0; current <= this.retries; current++ {
		response, err := this.inner.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			return response, nil
		} else if err == nil && response.StatusCode == http.StatusNotFound {
			return response, nil
		} else if err != nil {
			this.logger.Println("[WARN] Unexpected response from target storage:", err)
		} else if response.Body != nil {
			this.logger.Printf("[WARN] Target host rejected request ('%s'):\n%s\n", request.URL.Path, readResponse(response))
		}
		this.sleeper.Sleep(time.Second * 5)
	}
	return nil, errors.New("Max retries exceeded. Unable to connect.")
}
