package micro

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	micropb "gitlab.com/kickstar/backend/sdk-go/base/pb/micro"
	"gitlab.com/kickstar/backend/sdk-go/utils"
	"google.golang.org/grpc/metadata"

	//"gitlab.com/kickstar/backend/sdk-go/jwt"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	e "gitlab.com/kickstar/backend/sdk-go/base/error"
	"gitlab.com/kickstar/backend/sdk-go/utils/transform"
	"google.golang.org/grpc"
)

// Client object
type MicroClient struct {
	Id string
	//key-value store management
	//Config *vault.Vault
	Remote_service_address string
	Service                micropb.MicroServiceClient
}

func (c *MicroClient) CallService(ctx context.Context, method string, data map[string]interface{}, args ...interface{}) (*micropb.MicroResponse, *e.Error) {
	time_out := 30 //mins
	time_out_cfg := os.Getenv("TIMEOUT")
	if time_out_cfg != "" {
		time_out = utils.StringToInt(time_out_cfg)
	}
	if time_out <= 0 {
		time_out = 30
	}
	//
	if method == "" {
		return nil, e.New("Service Method is empty", "MICRO_CLIENT", "CALL_SERVICE")
	}
	if data == nil {
		return nil, e.New("Request parameter is empty", "MICRO_CLIENT", "CALL_SERVICE")
	}
	s, err := transform.MapToPBStruct(data)
	if err != nil {
		return nil, e.New(fmt.Sprintf("%s %s", "Convert Request parameter to Protobuf Struct error: ", err.Error()), "REQUEST_PARAMETER")
	}
	//create request parameter
	req := &micropb.MicroRequest{
		Token:      "xxxxxxxxxx",
		MethodName: method,
		Data:       s,
	}
	//if client not implement connection before then create connection
	maxMsgSize := 1024 * 1024 * 1024 //1GB
	//
	if c.Service == nil {
		transportOption := grpc.WithInsecure()
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(time_out)*60*time.Second)
		conn, err := grpc.DialContext(ctx, c.Remote_service_address,
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize),
				grpc.MaxCallSendMsgSize(maxMsgSize)),
			transportOption)
		if err != nil {
			return nil, e.New(err.Error(), "MICRO_CLIENT")
		}
		c.Service = micropb.NewMicroServiceClient(conn)
	}
	//
	maxSizeOption := grpc.MaxCallRecvMsgSize(maxMsgSize)
	//

	//
	resp, err := c.Service.Handler(CtxWithToken(ctx, utils.ItoString(data["token"])), req, maxSizeOption)
	if err != nil {
		return nil, e.New(err.Error(), "REQUEST_SERVICE")
	}
	return resp, nil
}

// NewClient returns a new client with a connection to remote service
func NewMicroClient(grpc_server_address string) (*MicroClient, *e.Error) {
	time_out := 30 //mins
	time_out_cfg := os.Getenv("TIMEOUT")
	if time_out_cfg != "" {
		time_out = utils.StringToInt(time_out_cfg)
	}
	if time_out <= 0 {
		time_out = 30
	}
	if grpc_server_address == "" {
		return nil, e.New("RPC Server Address is empty", "MICRO_CLIENT")
	}
	if IsServiceName(grpc_server_address) {
		grpc_server_address = GetEnpoint(grpc_server_address)
	}
	arr := utils.Explode(grpc_server_address, ":")
	if len(arr) == 1 {
		grpc_server_address = fmt.Sprintf("%s:%s", grpc_server_address, "30000")
	}
	//
	transportOption := grpc.WithInsecure()
	maxMsgSize := 1024 * 1024 * 1024 //1GB
	//timout 30mins
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(time_out)*60*time.Second)
	conn, err := grpc.DialContext(ctx, grpc_server_address,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize)), transportOption)
	if err != nil {
		return nil, e.New(err.Error(), "MICRO_CLIENT")
	}
	//
	service := micropb.NewMicroServiceClient(conn)
	return &MicroClient{
		Id:                     uuid.New().String(),
		Remote_service_address: grpc_server_address,
		Service:                service,
	}, nil
}

// NewClient without connection
func NewMicroClientWithoutConnection(grpc_server_address string) (*MicroClient, *e.Error) {
	if grpc_server_address == "" {
		return nil, e.New("RPC Server Address is empty", "MICRO_CLIENT")
	}
	if IsServiceName(grpc_server_address) {
		grpc_server_address = GetEnpoint(grpc_server_address)
	}
	arr := utils.Explode(grpc_server_address, ":")
	if len(arr) == 1 {
		grpc_server_address = fmt.Sprintf("%s:%s", grpc_server_address, "30000")
	}
	return &MicroClient{
		Id:                     uuid.New().String(),
		Remote_service_address: grpc_server_address,
	}, nil
}
func GetEnpoint(service_name string) string {
	if IsServiceName(service_name) {
		service_name = strings.ReplaceAll(service_name, "micro.", "")
		//hot fix
		service_name = strings.ReplaceAll(service_name, "kickstar", "grpc")
		arr := utils.Explode(service_name, ".")
		arr = utils.ReverseStringArray(arr)
		prefix := arr[0 : len(arr)-4]
		suffix := arr[len(arr)-4 : len(arr)]
		return fmt.Sprintf("%s.%s", strings.Join(prefix, "--"), strings.Join(suffix, "."))
	}
	return ""
}
func IsServiceName(endpoint string) bool {
	arr := utils.Explode(endpoint, ".")
	if len(arr) < 5 {
		return false
	}
	if arr[0] == "micro" && arr[1] == "local" && arr[2] == "cluster" && arr[3] == "svc" {
		return true
	}
	return false
}

func CtxWithToken(ctx context.Context, token string, args ...string) context.Context {
	/*user_id:=""
	role_id:=0
	if len(args)>0{//serect key
		claims,err:=jwt.VerifyJWTToken(args[0],token)
		if err==nil{
			user_id=claims.UserID
			role_id=claims.RoleID
		}
	}*/
	md := metadata.Pairs(
		"authorization", fmt.Sprintf("%s %v", "bearer", token),
		//"user_id",user_id,
		//"role_id",utils.IntToS(role_id),
	)
	nCtx := metautils.NiceMD(md).ToOutgoing(ctx)
	return nCtx
}
func FowardCtx(ctx context.Context) context.Context {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		token = ""
	}
	return CtxWithToken(ctx, token)
}
