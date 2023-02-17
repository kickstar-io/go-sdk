package response

import (
	micropb "gitlab.com/kickstar/backend/go-sdk/base/pb/micro"
	"gitlab.com/kickstar/backend/go-sdk/utils"
	"gitlab.com/kickstar/backend/go-sdk/utils/transform"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

/*
func RaiseError(msg string) *micropb.MicroResponse{
	return &micropb.MicroResponse{
		Code: 1,
		Msg:  msg,
		Total: 0,
	}
}
func RaiseSuccess(msg string,args...interface{}) *micropb.MicroResponse{
	total:=int64(0)
	if len(args)>0{
		total=utils.ItoInt64(args[0])
	}
	return &micropb.MicroResponse{
		Code: 0,
		Msg:  msg,
		Total: total,
	}
}*/

func RaiseError(msg string) *micropb.MicroResponse {
	return &micropb.MicroResponse{
		Code:  1,
		Msg:   msg,
		Total: 0,
		Data:  nil,
	}
}
func RaiseSuccess(msg string, args ...interface{}) *micropb.MicroResponse {
	total := int64(0)
	var data *structpb.Struct
	/*if len(args)>0{
		if utils.Type(args[0])=="array"{
			m:=map[string]interface{}{
				"data":args[0],
			}
			data=m
		}else{
			data=args[0]
		}

	}*/
	if len(args) > 0 {
		m := map[string]interface{}{
			"data": args[0],
		}
		res, _ := transform.IToPBStruct(m)
		data = res
	}

	if len(args) > 1 {
		total = utils.ItoInt64(args[1])
	}

	return &micropb.MicroResponse{
		Code:  0,
		Msg:   msg,
		Total: total,
		Data:  data,
	}
}
