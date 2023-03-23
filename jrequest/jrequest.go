package jrequest

import (
	"encoding/json"
	"gopkg.in/alexcesaro/statsd.v2"
	"log"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/valyala/fasthttp"
)

// JRequest lib make request
type JRequest struct {
	StatusCode     int
	BodyByte       []byte
	Err            error
	metricsSkipper bool
	statsd         *StatsdConfig
	*fasthttp.Response
	req     *fasthttp.Request
	client  *fasthttp.Client
	Timeout *time.Duration
}

// NewHTTPTransport new instance
func (c *Client) NewJRequest() *JRequest {
	hts := &JRequest{
		client:  c.Client,
		req:     fasthttp.AcquireRequest(),
		statsd:  c.Statsd,
		Timeout: c.Timeout,
	}
	return hts
}

func (ts *JRequest) ReleaseConnection() {
	fasthttp.ReleaseRequest(ts.req)
	fasthttp.ReleaseResponse(ts.Response)
}

// GET method get
func (ts *JRequest) GET(url string) (*JRequest, error) {
	ts.req.Header.SetMethod("GET")
	err := ts.makeHttpRequest(url, nil, ts.Timeout)
	return ts, err
}

// GET method get
func (ts *JRequest) GETX(url string, timeOut time.Duration) (*JRequest, error) {
	ts.req.Header.SetMethod("GET")
	err := ts.makeHttpRequest(url, nil, &timeOut)
	return ts, err
}

// SetHeader custom header
func (ts *JRequest) SetHeader(key, value string) *JRequest {
	ts.req.Header.Add(key, value)
	return ts
}

// SetHeader custom header
func (ts *JRequest) SetRequestId(value string) *JRequest {
	ts.req.Header.Add(echo.HeaderXRequestID, value)
	return ts
}

// SetUserAgent custom agent
func (ts *JRequest) SetUserAgent(value string) *JRequest {
	if value != "" {
		ts.req.Header.SetUserAgent(value)
	} else {
		ts.req.Header.SetUserAgent(UserAgent)
	}
	return ts
}

// SetContentType custom type
func (ts *JRequest) SetContentType(value string) *JRequest {
	// ts.req.Header.Add("Accept", value)
	ts.req.Header.Add("Accept-Encoding", "gzip, deflate")
	ts.req.Header.Add("Cache-Control", "no-cache")
	ts.req.Header.SetContentType(value)
	return ts
}

// ContentTypeJSON custom header
func (ts *JRequest) ContentTypeJSON() *JRequest {
	return ts.SetContentType("application/json")
}

// POST method
func (ts *JRequest) POST(url string, body []byte) (*JRequest, error) {
	ts.req.Header.SetMethod("POST")
	err := ts.makeHttpRequest(url, body, nil)
	return ts, err
}

// POST method
func (ts *JRequest) POSTX(url string, body []byte, timeOut time.Duration) (*JRequest, error) {
	ts.req.Header.SetMethod("POST")
	err := ts.makeHttpRequest(url, body, &timeOut)
	return ts, err
}

// PUT method
func (ts *JRequest) PUT(url string, body []byte) (*JRequest, error) {
	ts.req.Header.SetMethod("PUT")
	err := ts.makeHttpRequest(url, body, nil)
	return ts, err
}

// PUT method
func (ts *JRequest) PUTX(url string, body []byte, timeOut time.Duration) (*JRequest, error) {
	ts.req.Header.SetMethod("PUT")
	err := ts.makeHttpRequest(url, body, &timeOut)
	return ts, err
}

// Decode body []byte to struct
func (ts *JRequest) Decode(jsonCtx interface{}) error {
	if ts.Err != nil {
		return ts.Err
	}
	err := json.Unmarshal(ts.BodyByte, &jsonCtx)
	if err != nil {
		log.Println(`[utils.JRequest] error:`, err)
	}
	return err
}

func (ts *JRequest) PATCH(url string, body []byte) (*JRequest, error) {
	ts.req.Header.SetMethod("PATCH")
	err := ts.makeHttpRequest(url, body, nil)
	return ts, err
}

func (ts *JRequest) PATCHX(url string, body []byte, timeOut time.Duration) (*JRequest, error) {
	ts.req.Header.SetMethod("PATCH")
	err := ts.makeHttpRequest(url, body, &timeOut)
	return ts, err
}

func (ts *JRequest) makeHttpRequest(url string, body []byte, timeout *time.Duration) error {
	ts.SetUserAgent(UserAgent)
	isStats := !ts.metricsSkipper && ts.statsd != nil
	var t statsd.Timing
	if isStats {
		t = ts.statsd.StatsdClient.NewTiming()
	}
	ts.req.SetRequestURI(url)
	ts.req.SetBody(body)
	ts.ContentTypeJSON()
	resp := fasthttp.AcquireResponse()
	if timeout == nil {
		if err := ts.client.Do(ts.req, resp); err != nil {
			ts.Err = err
			return err
		}
	} else {
		if err := ts.client.DoTimeout(ts.req, resp, *timeout); err != nil {
			return err
		}
	}
	if string(resp.Header.Peek("Content-Encoding")) == "gzip" {
		ts.BodyByte, _ = resp.BodyGunzip()
	} else {
		ts.BodyByte = resp.Body()
	}
	ts.StatusCode = resp.StatusCode()
	if isStats {
		t.Send(ts.statsd.StatsdTemplate(string(ts.req.Header.Method()), url, resp.StatusCode()))
	}
	defer fasthttp.ReleaseResponse(resp)
	defer fasthttp.ReleaseRequest(ts.req)
	return nil
}
