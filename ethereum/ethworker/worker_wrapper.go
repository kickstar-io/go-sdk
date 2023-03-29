package ethworker

import (
	"context"
	"math/big"

	"github.com/kickstar-io/go-sdk/plog"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var logger = plog.NewBizLogger("chain-worker")

type ChainEventHandler func(vlog types.Log)

func ListenEvent(host string, conAddr []string, block int64, logFuncHandler map[string]ChainEventHandler) {

	isExist := false
	defer func() {
		if isExist {
			logger.Info().Msg("event handler started")
			ListenEvent(host, conAddr, block, logFuncHandler)
		}
	}()

	client, err := ethclient.Dial(host)
	if err != nil {
		logger.Error().Err(err).Send()
		return
	}

	addresses := make([]common.Address, 0)
	for _, addr := range conAddr {
		contractAddress := common.HexToAddress(addr)
		addresses = append(addresses, contractAddress)
	}

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(block),
		Addresses: addresses,
	}

	logs := make(chan types.Log)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logs)
	if err != nil {
		logger.Error().Err(err).Send()
		return
	}

	for {
		select {
		case err := <-sub.Err():
			isExist = true
			logger.Error().Err(err).Send()
			return
		case vLog := <-logs:
			_, ok := logFuncHandler[vLog.Topics[0].Hex()]
			if ok {
				go logFuncHandler[vLog.Topics[0].Hex()](vLog)
			}
		}
	}
}
