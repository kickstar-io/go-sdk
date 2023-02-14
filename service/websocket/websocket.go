package websocket

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"context"
	//"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	e "gitlab.com/kickstar/sdk-go/base/error"
	"gitlab.com/kickstar/sdk-go/base/event"
	r "gitlab.com/kickstar/sdk-go/cache/redis"
	"gitlab.com/kickstar/sdk-go/config/vault"
	ed "gitlab.com/kickstar/sdk-go/eventdriven"

	//j "gitlab.com/kickstar/sdk-go/jwt"
	"os/signal"
	"time"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"gitlab.com/kickstar/sdk-go/log"
	"gitlab.com/kickstar/sdk-go/service/micro"
	"gitlab.com/kickstar/sdk-go/utils"
)

type Websocket struct {
	Id           string
	host         string
	port         string
	service_name string
	config       *vault.Vault
	key          string
	Srv          *echo.Echo
	Hub          *Hub
	//event driven subscriber, only 1
	Pub        map[string]*ed.EventDriven
	NetworkSub ed.EventDriven
	Sub        map[string]*ed.EventDriven
	//micro client
	Client map[string]*micro.MicroClient
	// Redis
	rd r.CacheHelper
}

func (w *Websocket) Initial(service_name string, wsHandleFunc echo.HandlerFunc, mapSubCallbackfn map[string]event.ConsumeFn, args ...interface{}) {
	w.Id = uuid.NewString()
	err := godotenv.Load(os.ExpandEnv("/config/.env"))
	if err != nil {
		err := godotenv.Load(os.ExpandEnv(".env"))
		if err != nil {
			panic(err)
		}
	}
	log.Initial(service_name)
	w.service_name = service_name

	w.Hub = newHub()

	//initial Server configuration
	var config vault.Vault
	w.config = &config
	w.config.Initial(service_name)

	//read secret key for generate JWT
	w.key = w.config.ReadVAR("key/api/KEY")

	if os.Getenv("HTTP_HOST") == "" {
		w.host = "0.0.0.0"
	} else {
		w.host = os.Getenv("HTTP_HOST")
	}
	if os.Getenv("HTTP_PORT") != "" {
		w.port = os.Getenv("HTTP_PORT")
	} else if w.host == "" {
		w.port = "8080"
	}

	w.Srv = echo.New()
	w.Srv.HideBanner = true
	w.Srv.HidePort = true

	w.Srv.Use(middleware.Logger())
	w.Srv.Use(middleware.Recover())
	w.Srv.GET("/ws", wsHandleFunc)
	/*
		config_jwt := middleware.JWTConfig{
			Claims:        &j.CustomClaims{},
			SigningKey:    []byte(w.key),
			SigningMethod: jwt.SigningMethodHS256.Name,
			TokenLookup:   "query:token",
			AuthScheme:    "Bearer",
			Skipper: func(c echo.Context) bool {
				token := c.QueryParam("token")
				return token == ""
			},
			ParseTokenFunc: func(token string, c echo.Context) (interface{}, error) {
				if os.Getenv("IGNORE_TOKEN") == "true" {
					return nil, nil
				}
				claims_info, err := j.VerifyJWTToken(w.key, token)
				if err != nil {
					return nil, err
				}
				return claims_info, nil
			},
		}
		w.Srv.Use(middleware.JWTWithConfig(config_jwt))
	*/
	// Init redis
	redis, err_r := r.NewCacheHelper(w.config)
	if err_r != nil {
		log.ErrorF(err_r.Msg())
	}
	w.rd = redis

	// init Subscriber
	var err_s *e.Error
	w.Sub = make(map[string]*ed.EventDriven)
	check, err_p := w.config.CheckPathExist("websocket/" + service_name + "/sub/kafka")
	if err_p != nil {
		log.ErrorF(err_p.Msg(), w.config.GetServiceName())
	}
	if check {
		event_list := w.config.ListItemByPath("websocket/" + service_name + "/sub/kafka")
		for _, event := range event_list {
			if callback, ok := mapSubCallbackfn[event]; ok {
				isMultiTopic := w.config.ReadVAR("websocket/" + service_name + "/sub/kafka/" + event + "/MULTI_TOPIC")
				if isMultiTopic == "true" {
					subEvents := w.config.ReadVAR("websocket/" + service_name + "/sub/kafka/" + event + "/TOPIC")
					eventList := strings.Split(subEvents, ",")
					for _, subEvent := range eventList {
						w.Sub[subEvent] = &ed.EventDriven{}
						err_s = w.Sub[subEvent].InitialSubscriberWithGlobal(w.config, fmt.Sprintf("websocket/%s/%s/%s", service_name, "sub/kafka", subEvent), service_name, callback, nil, subEvent)
						if err_s != nil {
							log.ErrorF(err_s.Msg(), err_s.Group(), err_s.Key())
						}
					}
				} else {
					w.Sub[event] = &ed.EventDriven{}
					err_s = w.Sub[event].InitialSubscriberWithGlobal(w.config, fmt.Sprintf("websocket/%s/%s/%s", service_name, "sub/kafka", event), service_name, callback, nil, "")
					if err_s != nil {
						log.ErrorF(err_s.Msg(), err_s.Group(), err_s.Key())
					}
				}
			}
		}
	}

	check, err_p = w.config.CheckPathExist("websocket/" + service_name + "/pub/kafka")
	if err_p != nil {
		log.ErrorF(err_p.Msg(), w.config.GetServiceName())
	}
	if check {
		w.Pub = make(map[string]*ed.EventDriven)
		event_list := w.config.ListItemByPath("websocket/" + service_name + "/pub/kafka")
		for _, event := range event_list {
			if !Map_PublisherContains(w.Pub, event) && event != "general" {
				w.Pub[event] = &ed.EventDriven{}
				err := w.Pub[event].InitialPublisherWithGlobal(w.config, fmt.Sprintf("websocket/%s/%s/%s", service_name, "pub/kafka", event), service_name, event)
				if err != nil {
					log.ErrorF(err.Msg(), service_name, "Initial")
				}
			}
		}

	}

	//micro client call service
	if len(args) > 0 {
		if args[0] != nil {
			remote_services, err := utils.ItoDictionaryS(args[0])
			if err != nil {
				log.ErrorF(err.Error(), "WORKER", "INITIAL_CONVERTION_MODEL")
			} else {
				w.InitialMicroClient(remote_services, false)
			}
		}
	}

}

func (w *Websocket) Start() {
	if w.host == "" || w.port == "" {
		log.Error("Please Initial before make new server")
		os.Exit(0)
	}
	fmt.Printf("HTTP Server start at: %s:%s\n", w.host, w.port)
	go w.Hub.run()
	// Start server
	for _, sub := range w.Sub {
		go func(sub *ed.EventDriven) {
			if err := sub.Subscribe(); err != nil {
				log.ErrorF(err.Msg(), err.Group(), err.Key())
			}
		}(sub)

	}
	go func() {
		if err := w.Srv.Start(":" + w.port); err != nil && err != http.ErrServerClosed {
			w.Srv.Logger.Fatal("shutting down the server")
		}
	}()
	//gracefull shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Srv.Shutdown(ctx); err != nil {
		w.Srv.Logger.Fatal(err)
	}
}

func (w *Websocket) GetConfig() *vault.Vault {
	return w.config
}

func (w *Websocket) GetServiceName() string {
	return w.service_name
}

func Map_PublisherContains(m map[string]*ed.EventDriven, item string) bool {
	if len(m) == 0 {
		return false
	}
	if _, ok := m[item]; ok {
		return true
	}
	return false
}

func (w *Websocket) InitialMicroClient(remote_services map[string]string, initConnection bool) {
	w.Client = make(map[string]*micro.MicroClient)
	var err *e.Error
	for k, v := range remote_services {
		if k != "" {
			if initConnection {
				w.Client[k], err = micro.NewMicroClient(v)
				if err != nil {
					log.ErrorF(err.Msg(), err.Group(), err.Key())
				} else {
					log.Info(fmt.Sprintf("Micro Client: %s->%s %s", k, v, " Initial success"))
				}
			} else {
				w.Client[k], err = micro.NewMicroClientWithoutConnection(v)
				if err != nil {
					log.ErrorF(err.Msg(), err.Group(), err.Key())
				} else {
					log.Info(fmt.Sprintf("Micro Client: %s->%s %s", k, v, " Initial success"))
				}
			}
		}
	}
}
