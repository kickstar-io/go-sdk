package firebase

import (
	"errors"
	"net/http"
	"time"
)

// Option configurates Client with defined option.
type Option func(*Client) error

// Return Option to configure FCM Endpoint.
func WithEndpoint(endpoint string) Option {
	return func(c *Client) error {
		if endpoint == "" {
			return errors.New("invalid endpoint")
		}
		c.endpoint = endpoint
		return nil
	}
}

//Return Option to configure HTTP Client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		c.client = httpClient
		return nil
	}
}

// Return Option to configure HTTP Client timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) error {
		if d.Nanoseconds() <= 0 {
			return errors.New("invalid timeout duration")
		}
		c.timeout = d
		return nil
	}
}
