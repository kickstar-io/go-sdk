package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/kickstar-io/go-sdk/jwt"
	"github.com/kickstar-io/go-sdkgo-sdk/plog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type grpcServer struct {
	host      string
	port      string
	name      string
	tokenKey  string
	service   *grpc.Server
	whitelist []string
}

var grpcInstance *grpcServer
var log = plog.NewBizLogger("grpc")

// init service grpc
func Initial(name, host, port, tokenKey string, whitelist []string) {

	if grpcInstance != nil {
		log.Warn().Msg("grpc server is alrealdy declare")
		return
	}

	// define new grpc client
	grpcInstance = &grpcServer{}

	grpcInstance.port = port
	grpcInstance.host = host
	grpcInstance.name = name
	grpcInstance.tokenKey = tokenKey
	grpcInstance.whitelist = whitelist

	maxMsgSize := 1024 * 1024 * 1024 //1GB
	grpcInstance.service = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(authFunc)), //middleware verify authen
	)
}

// start service grpc
func Start() {
	if grpcInstance == nil {
		log.Error().Msg("please Initial before make new server")
		os.Exit(0)
	}

	errs_chan := make(chan error)
	stop_chan := make(chan os.Signal)

	// bind OS events to the signal channel
	signal.Notify(stop_chan, syscall.SIGTERM, syscall.SIGINT)

	go listen(errs_chan)

	defer func() {
		grpcInstance.service.GracefulStop()
	}()

	// block until either OS signal, or server fatal error
	select {
	case err := <-errs_chan:
		log.Error().Err(err).Msg("err chan")
	case <-stop_chan:
	}

	log.Warn().Msg("Service shutdown")
}

func listen(errs chan error) {

	grpcAddr := net.JoinHostPort(grpcInstance.host, grpcInstance.port)
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error().Err(err).Msg("listener err")
	}

	log.Info().Msg(fmt.Sprintf("gRPC service started: %s - %s", grpcInstance.name, grpcAddr))

	errs <- grpcInstance.service.Serve(listener)
}

// GetService get grpc service
func GetService() *grpc.Server {
	return grpcInstance.service
}

func authFunc(ctx context.Context) (context.Context, error) {

	//ignore check token
	if os.Getenv("IGNORE_TOKEN") == "true" {
		return ctx, nil
	}

	//verify permision base on service name + method name
	method_route, ok := grpc.Method(ctx)
	if !ok {
		return nil, errors.New("ACL_ACCESS_DENY")
	}

	log.Info().Var("grpc_route", method_route).Send()

	if method_route == "" {
		return nil, errors.New("ACL_METHOD_EMPTY")
	}

	// check whitelist route
	for _, e := range grpcInstance.whitelist {
		if strings.ToLower(method_route) == strings.ToLower(e) {
			return ctx, nil
		}
	}

	// get jwt token
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	// check token is empty
	if token == "" {
		return nil, errors.New("_TOKEN_IS_EMPTY_")
	}

	// verify token
	claims, err := jwt.VerifyJWTToken(grpcInstance.tokenKey, token)
	if err != nil {
		return nil, err
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if len(md.Get("userid")) == 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "userid", claims.UserID)
		}
		if len(md.Get("roleid")) == 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "roleid", strconv.Itoa(claims.RoleID))
		}
	}
	return ctx, nil
}
