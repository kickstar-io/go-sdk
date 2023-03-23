package jrequest

import (
	"fmt"
	"gopkg.in/alexcesaro/statsd.v2"
	"net/url"
	"os"
	"strings"
)

type (
	ProfilerConfig struct {
		Skipper bool
		Address string
		Service string
	}
)

type StatsdConfig struct {
	StatsdClient *statsd.Client
	Skipper      bool
	Address      string
	Service      string
}

var (
	// DefaultBodyLimitConfig is the default Gzip middleware config.
	DefaultProfilerConfig = ProfilerConfig{
		Skipper: false,
		Address: ":8125",
		Service: "default",
	}
	// client statsd.Client
)

func InitStatsd(config *ProfilerConfig) (*StatsdConfig, error) {
	config.Skipper = DefaultProfilerConfig.Skipper

	if config.Address == "" {
		config.Address = DefaultProfilerConfig.Address
	}
	if config.Service == "" {
		config.Service = DefaultProfilerConfig.Service
	}
	client, err := statsd.New(statsd.Address(config.Address))
	if err != nil {
		fmt.Printf("Failed to initialized statsd Client %s\n", err)
		return nil, err
	}
	fmt.Println("Connect success")
	return &StatsdConfig{
		StatsdClient: client,
		Skipper:      config.Skipper,
		Address:      config.Address,
		Service:      config.Service,
	}, nil
}

func (config *StatsdConfig) StatsdTemplate(method string, rawurl string, status int) string {
	//Replace : to #, https://github.com/influxdata/telegraf/pull/3514
	urlParse, _ := url.Parse(rawurl)
	path := strings.Replace(urlParse.Hostname()+urlParse.Path, ".", "*", -1)
	s := strings.ToLower(fmt.Sprintf("3pl.%s.%s.%s.%d", config.Service, method, path, status))

	if os.Getenv("LOG_LEVEL") == "debug" {
		fmt.Println(s)
	}
	return s
}
