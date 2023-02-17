package worker

import (
	e "gitlab.com/kickstar/backend/go-sdk/base/error"
	"gitlab.com/kickstar/backend/go-sdk/config/vault"
	ed "gitlab.com/kickstar/backend/go-sdk/eventdriven"
	"gitlab.com/kickstar/backend/go-sdk/log"

	//"gitlab.com/kickstar/backend/go-sdk/pubsub/kafka"
	"gitlab.com/kickstar/backend/go-sdk/db"
	"gitlab.com/kickstar/backend/go-sdk/db/mongo"
	"gitlab.com/kickstar/backend/go-sdk/pubsub/kafka"
	"gitlab.com/kickstar/backend/go-sdk/service/micro"
	"gitlab.com/kickstar/backend/go-sdk/utils"

	//"gitlab.com/kickstar/backend/go-sdk/db/mongo/status"
	//e "gitlab.com/kickstar/backend/go-sdk/base/error"
	"fmt"
	"os"

	"github.com/joho/godotenv"
	//"errors"
)

type Worker struct {
	//worker name
	worker_name string
	//key-value store management
	config *vault.Vault
	//db map[string]dbconnection
	Mgo db.MongoDB
	//log publisher
	log_pub ed.EventDriven
	//micro client
	Client map[string]*micro.MicroClient
}

// initial n worker
func (w *Worker) Initial(worker_name string, args ...interface{}) {
	//get ENV
	err := godotenv.Load(os.ExpandEnv("/config/.env"))
	if err != nil {
		err := godotenv.Load(os.ExpandEnv(".env"))
		if err != nil {
			panic(err)
		}
	}
	log.Initial(worker_name)
	w.worker_name = worker_name
	//initial Server configuration
	var config vault.Vault
	w.config = &config
	w.config.Initial(fmt.Sprintf("%s/%s", "worker", worker_name))
	//
	//ReInitial Destination for Logger
	if log.LogMode() != 2 { // not in local, locall just output log to std
		log_dest := w.config.ReadVAR("logger/general/LOG_DEST")
		if log_dest == "kafka" {
			config_map := kafka.GetConfig(w.config, "logger/kafka")
			log.SetDestKafka(config_map)
		}
	}
	//init consumed Log
	//initial DB args[0] => mongodb
	if len(args) > 1 {
		if args[1] != nil {
			models, err := utils.ItoDictionary(args[1])
			if err != nil {
				log.ErrorF(err.Error(), "WOKER", "INITIAL_CONVERTION_MODEL")
			}
			err_init := w.Mgo.Initial(w.config, models)
			if err_init != nil {
				log.Warn(err_init.Msg(), err_init.Group(), err_init.Key())
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

func (w *Worker) GetServiceName(eventName string) (string, error) {
	return utils.GetServiceName(eventName)
}
func (w *Worker) GetServiceMethod(eventName string) (string, error) {
	return utils.GetServiceMethod(eventName)
}
func (w *Worker) InitialMicroClient(remote_services map[string]string, initConnection bool) {
	//
	w.Client = make(map[string]*micro.MicroClient)
	//
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
	//
}

func (w *Worker) GetDB(col_name string) (*mongo.Collection, *e.Error) {
	if w.Mgo.Cols[col_name] != nil {
		return w.Mgo.Cols[col_name], nil
	} else {
		return nil, e.New(fmt.Sprintf(" DB not initial: %s", col_name), "Worker", "GetDB")
	}
}
func (w *Worker) GetClient(client_name string) (*micro.MicroClient, *e.Error) {
	if w.Client[client_name] != nil {
		return w.Client[client_name], nil
	} else {
		return nil, e.New(fmt.Sprintf(" Micro Client not initial: %s", client_name), "Worker", "GetClient")
	}
}
func (w *Worker) GetConfig() *vault.Vault {
	return w.config
}
func (w *Worker) Clean() {
	w.Mgo.Clean()
}
