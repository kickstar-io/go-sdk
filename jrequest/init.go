package jrequest

import (
	"github.com/valyala/fasthttp"
	"time"
)

const (
	// UserAgent default
	UserAgent = "Aha-JRequest-Client"
)

type Client struct {
	Client         *fasthttp.Client
	Statsd         *StatsdConfig
	ProfilerConfig *ProfilerConfig
	Timeout        *time.Duration
}

func DefaultClient() *Client {
	timeout := time.Duration(30) * time.Second
	return &Client{
		Client:         &fasthttp.Client{},
		Statsd:         nil,
		ProfilerConfig: nil,
		Timeout:        &timeout,
	}
}
