package firebase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var Firebase *Client

var (
	// Endpoint of fcm server
	Endpoint = "https://fcm.googleapis.com/fcm/send"
	// Timeout duration in second
	Timeout time.Duration = 30 * time.Second
)

// To send a message to one or more devices use the Client's Send.
type Client struct {
	apiKey   string
	client   *http.Client
	endpoint string
	timeout  time.Duration
}

// Create new Firebase Cloud Messaging Client based on API key and with default endpoint and http client.
func NewClient(apiKey string, opts ...Option) (*Client, error) {
	if apiKey == "" {
		return nil, ErrorInvalidAPIKey
	}
	c := &Client{
		apiKey:   apiKey,
		endpoint: Endpoint,
		client:   &http.Client{},
		timeout:  Timeout,
	}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Send a message to the FCM server without retrying
func (c *Client) SendWithContext(ctx context.Context, msg *Message) (*Response, error) {
	// validate
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	// marshal message
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return c.send(ctx, data)
}

// Send a message to the FCM server without retrying
func (c *Client) Send(msg *Message) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.SendWithContext(ctx, msg)
}

// Send a message to the FCM server with defined number of retrying
func (c *Client) SendWithRetry(msg *Message, retryAttempts int) (*Response, error) {
	return c.SendWithRetryWithContext(context.Background(), msg, retryAttempts)
}

// Send a message to the FCM server with defined number of retrying, uses external context.
func (c *Client) SendWithRetryWithContext(ctx context.Context, msg *Message, retryAttempts int) (*Response, error) {
	// validate
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	// marshal message
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	resp := new(Response)
	err = retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		var er error
		resp, er = c.send(ctx, data)
		return er
	}, retryAttempts)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Send a message.
func (c *Client) send(ctx context.Context, data []byte) (*Response, error) {
	// create request
	req, err := http.NewRequest("POST", c.endpoint, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	// add headers
	req.Header.Add("Authorization", fmt.Sprintf("key=%s", c.apiKey))
	req.Header.Add("Content-Type", "application/json")
	// execute request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, connectionError(err.Error())
	}
	defer resp.Body.Close()
	// check response status
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= http.StatusInternalServerError {
			return nil, serverError(fmt.Sprintf("%d error: %s", resp.StatusCode, resp.Status))
		}
		return nil, fmt.Errorf("%d error: %s", resp.StatusCode, resp.Status)
	}
	// build return response
	response := new(Response)
	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return nil, err
	}
	return response, nil
}
