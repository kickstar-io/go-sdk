package grpc

import (
	"net"
	"os"

	"gitlab.com/kickstar/backend/go-sdk/jwt"
	"gitlab.com/kickstar/backend/go-sdk/log"
	"gitlab.com/kickstar/backend/go-sdk/utils"
	"google.golang.org/grpc/metadata"

	//"gitlab.com/kickstar/backend/go-sdk/utils"
	"fmt"

	"github.com/joho/godotenv"
	"gitlab.com/kickstar/backend/go-sdk/cache/redis"
	"gitlab.com/kickstar/backend/go-sdk/config/vault"
	"gitlab.com/kickstar/backend/go-sdk/pubsub/kafka"
	"google.golang.org/grpc"

	//e "gitlab.com/kickstar/backend/go-sdk/base/error"
	"context"
	"errors"
	"os/signal"
	"syscall"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
)

type GRPCServer struct {
	host        string
	port        string
	servicename string

	//key-value store management
	config *vault.Vault

	//grpc server
	service    *grpc.Server
	two_FA_Key string
	token_Key  string

	//ACL db store
	Redis redis.CacheHelper
	//ACL cache
	acl map[string]bool

	whitelist []string
}

func (g *GRPCServer) Initial(service_name string) {
	//get ENV
	err := godotenv.Load(os.ExpandEnv("/config/.env"))
	if err != nil {
		err := godotenv.Load(os.ExpandEnv(".env"))
		if err != nil {
			panic(err)
		}
	}

	// initial logger
	log.Initial(service_name)

	//initial Server configuration
	var config vault.Vault
	g.config = &config
	g.config.InitialByToken(service_name)

	//get config from key-value store
	port_env := g.config.ReadVAR("grpc/general/GRPC_PORT")
	if port_env != "" {
		g.port = port_env
	}

	host_env := g.config.ReadVAR("grpc/general/GRPC_HOST")
	if host_env != "" {
		g.host = host_env
	}

	//set service name
	g.servicename = service_name
	//ReInitial Destination for Logger
	if log.LogMode() != 2 { // not in local, locall just output log to std
		log_dest := g.config.ReadVAR("logger/general/LOG_DEST")
		if log_dest == "kafka" {
			config_map := kafka.GetConfig(g.config, "logger/kafka")
			log.SetDestKafka(config_map)
		}
	}

	//new grpc server
	maxMsgSize := 1024 * 1024 * 1024 //1GB
	//read 2FA Key for verify token
	g.two_FA_Key = g.config.ReadVAR("key/2fa/KEY")
	g.token_Key = g.config.ReadVAR("key/api/KEY")

	g.service = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(g.authFunc)), //middleware verify authen
	)

}

func (g *GRPCServer) AddWhitelist(whitelist []string) {
	g.whitelist = whitelist
}

/*
Start gRPC server with IP:Port from Initial step
*/
func (grpcSRV *GRPCServer) Start() {
	//check server
	if grpcSRV.host == "" || grpcSRV.port == "" {
		log.Error("Please Initial before make new server")
		os.Exit(0)
	}
	errs_chan := make(chan error)
	stop_chan := make(chan os.Signal)
	// bind OS events to the signal channel
	signal.Notify(stop_chan, syscall.SIGTERM, syscall.SIGINT)
	//
	go grpcSRV.listen(errs_chan)
	//
	defer func() {
		grpcSRV.service.GracefulStop()
	}()
	// block until either OS signal, or server fatal error
	select {
	case err := <-errs_chan:
		log.Error(err.Error(), "MICRO")
	case <-stop_chan:
	}
	log.Warn("Service shutdown", "MICRO")

}
func (grpcSRV *GRPCServer) listen(errs chan error) {
	grpcAddr := net.JoinHostPort(grpcSRV.host, grpcSRV.port)
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.ErrorF(err.Error())
	}
	//
	log.Info(fmt.Sprintf("gRPC service started: %s - %s", grpcSRV.servicename, grpcAddr))
	//
	//healthgrpc.RegisterHealthSever(server, hs)
	//reflection.Register(server)
	errs <- grpcSRV.service.Serve(listener)
}
func (grpcSRV *GRPCServer) GetService() *grpc.Server {
	return grpcSRV.service
}
func (grpcSRV *GRPCServer) GetConfig() *vault.Vault {

	return grpcSRV.config
}

func (grpcSRV *GRPCServer) authFunc(ctx context.Context) (context.Context, error) {
	//fmt.Println(grpcSRV.servicename)

	//ignore check token
	if os.Getenv("IGNORE_TOKEN") == "true" {
		return ctx, nil
	}
	//
	//verify permision base on service name + method name
	method_route, res := grpc.Method(ctx)

	fmt.Println("gRPC Route: ", method_route)

	if !res {
		return nil, errors.New("_ACL_ACCESS_DENY_")
	}
	if method_route == "" {
		return nil, errors.New("_ACL_METHOD_EMPTY_")
	}
	//ignore check token if method in whitelist
	if utils.Contains(grpcSRV.whitelist, method_route) {
		return ctx, nil
	}
	/*
		arr:=utils.Explode(method_route,"/")
		if len(arr)!=2{
			return nil,errors.New("_ACL_METHOD_INVALID_")
		}
		method:=arr[2]
		//whitelist ACL: servicename_method
		acl_key_all:=grpcSRV.servicename+"_"+method
		if utils.MapB_contains(grpcSRV.acl,acl_key_all){
			return ctx,nil
		}else{
			check, err := grpcSRV.Redis.Exists(acl_key_all)
			if err!=nil{
				return ctx,errors.New("_REDIS_ERROR_CANNOT_VERIFY_ACL_")
			}
			if check{
				grpcSRV.acl[acl_key_all]=true//store cache
				return ctx,nil
			}
		}
	*/
	//
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	//fmt.Println(token)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, errors.New("_TOKEN_IS_EMPTY_")
	}
	claims, err_v := jwt.VerifyJWTToken(grpcSRV.token_Key, token)
	if err_v != nil {
		return nil, err_v
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if len(md.Get("userid")) == 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "userid", claims.UserID)
		}
		if len(md.Get("roleid")) == 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "roleid", utils.IntToS(claims.RoleID))
		}
	}
	return ctx, nil
}
