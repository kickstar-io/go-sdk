package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"gitlab.com/kickstar/backend/go-sdk/config/vault"
	"gitlab.com/kickstar/backend/go-sdk/db"
	ed "gitlab.com/kickstar/backend/go-sdk/eventdriven"
	j "gitlab.com/kickstar/backend/go-sdk/jwt"
	"gitlab.com/kickstar/backend/go-sdk/log"
	"gitlab.com/kickstar/backend/go-sdk/pubsub/kafka"
	"gitlab.com/kickstar/backend/go-sdk/service/micro"
	"gitlab.com/kickstar/backend/go-sdk/utils"
)

type HTTPServer struct {
	host        string
	port        string
	servicename string
	//key-value store management
	config *vault.Vault
	//http server
	Srv *echo.Echo
	//db map[string]dbconnection
	Mgo db.MongoDB
	//event
	Pub map[string]*ed.EventDriven
	//micro client
	Client map[string]*micro.MicroClient
	//serect key for JWT
	key       string
	whitelist []string
	//ACL
	Acl map[string]interface{}
}

func (sv *HTTPServer) Initial(service_name string) {
	//get ENV
	err := godotenv.Load(os.ExpandEnv("/config/.env"))
	if err != nil {
		err := godotenv.Load(os.ExpandEnv(".env"))
		if err != nil {
			panic(err)
		}
	}

	//initial logger
	log.Initial(service_name)

	//initial Server configuration
	var config vault.Vault
	sv.config = &config
	sv.config.InitialByToken(service_name)

	//get config from key-value store
	http_port_env := sv.config.ReadVAR("api/general/HTTP_PORT")
	if http_port_env != "" {
		sv.port = http_port_env
	}
	http_host_env := sv.config.ReadVAR("api/general/HTTP_HOST")
	if http_host_env != "" {
		sv.host = http_host_env
	}

	//set service name
	sv.servicename = service_name
	//ReInitial Destination for Logger
	if log.LogMode() != 2 { // not in local, locall just output log to std
		log_dest := sv.config.ReadVAR("logger/general/LOG_DEST")
		if log_dest == "kafka" {
			config_map := kafka.GetConfig(sv.config, "logger/kafka")
			log.SetDestKafka(config_map)
		}
	}

	//read secret key for generate JWT
	sv.key = sv.config.ReadVAR("key/jwt/JWT_TOKEN")

	//new server
	sv.Srv = echo.New()
	sv.Srv.HideBanner = true
	sv.Srv.HidePort = true

	// add common middleware
	sv.Srv.Use(middleware.Logger())
	sv.Srv.Use(middleware.Recover())

	// add jwt middleware
	config_jwt := middleware.JWTConfig{
		Claims:        &j.CustomClaims{},
		SigningKey:    []byte(sv.key),
		SigningMethod: jwt.SigningMethodHS512.Name,
		//ContextKey:       "user",
		TokenLookup: "header:" + echo.HeaderAuthorization + ",query:token",
		//TokenLookupFuncs: nil,
		AuthScheme: "Bearer",
		//KeyFunc:          nil,
		Skipper: func(c echo.Context) bool {
			if os.Getenv("IGNORE_TOKEN") == "true" {
				return true
			}
			if os.Getenv("ENV") == "local" {
				fmt.Println("Route: ", c.Request().URL.Path)
			}

			// add whitelist route
			// if utils.Contains(routes_ignore_jwt, c.Request().URL.Path) {
			// 	return true
			// }
			return false
		},
		ParseTokenFunc: func(token string, c echo.Context) (interface{}, error) {
			if os.Getenv("IGNORE_TOKEN") == "true" {
				return nil, nil
			}
			if os.Getenv("ENV") == "local" {
				fmt.Println("KEY", sv.key, "Token: ", token)
			}

			Claims_info, err := j.VerifyJWTToken(sv.key, token)
			if err != nil {
				return nil, err
			}
			if utils.MapI_contains(sv.Acl, utils.ItoString(Claims_info.RoleID)) {
				m_acl, err := utils.ItoDictionaryBool(sv.Acl[utils.ItoString(Claims_info.RoleID)])
				if err != nil {
					return nil, err
				}
				if m_acl[c.Request().URL.Path] {
					return token, nil
				}
			}
			return token, nil
		},
	}

	//disable CORS
	sv.Srv.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{"*"},
		AllowMethods: []string{"*"},
	}))
	sv.Srv.Use(middleware.JWTWithConfig(config_jwt))
}

// add list whitelist route
func (sv *HTTPServer) AddWhitelistRoute(routes []string) {
	sv.whitelist = routes
}

// start
func (sv *HTTPServer) Start() {
	if sv.host == "" || sv.port == "" {
		log.Error("Please Initial before make new server")
		os.Exit(0)
	}
	//
	fmt.Println(fmt.Sprintf("HTTP Server start at: %s:%s", sv.host, sv.port))
	// Start server
	go func() {
		if err := sv.Srv.Start(":" + sv.port); err != nil && err != http.ErrServerClosed {
			sv.Srv.Logger.Fatal("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
	// Use a buffered channel to avoid missing signals as recommended for signal.Notify
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sv.Srv.Shutdown(ctx); err != nil {
		sv.Srv.Logger.Fatal(err)
	}
}

func (sv *HTTPServer) SetPathKey(path string) error {
	key := sv.config.ReadVAR(path)
	if key == "" {
		return errors.New("_PATH_KEY_NOT_EXISTS_")
	}
	sv.key = path
	return nil
}

func (sv *HTTPServer) GetUserID(c echo.Context) (string, error) {
	token, err := sv.GetToken(c)
	if err != nil {
		return "", err
	}
	Claims_info, err := j.VerifyJWTToken(sv.key, token)
	if err != nil {
		return "", err
	}
	return Claims_info.UserID, nil
}

func (sv *HTTPServer) GetRoleID(c echo.Context) (int, error) {
	token, err := sv.GetToken(c)
	if err != nil {
		return 0, err
	}
	Claims_info, err := j.VerifyJWTToken(sv.key, token)
	if err != nil {
		return 0, err
	}
	return Claims_info.RoleID, nil
}

func (sv *HTTPServer) GetToken(c echo.Context) (string, error) {
	authen_str := c.Request().Header.Get("Authorization")
	arr := utils.Explode(authen_str, " ")
	if len(arr) != 2 {
		token := c.QueryParam("token")
		if token == "" {
			return "", errors.New("_JWT_INVALID_")
		}
		return token, nil
	}
	return arr[1], nil
}
func (sv *HTTPServer) Restricted(c echo.Context) error {
	//user := c.Get("user").(*jwt.Token)
	//claims := user.Claims.(*jwt.CustomClaims)
	//name := claims.Name
	//return c.String(http.StatusOK, "Welcome "+name+"!")
	return errors.New("Access Deny")
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
