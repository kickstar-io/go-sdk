package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ponlv/go-kit/ethereum/ethutils"
	"github.com/ponlv/go-kit/plog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	client              *ethclient.Client
	chainId             *big.Int
	HardGasPrice        = big.NewInt(0).Mul(big.NewInt(100), big.NewInt(0).Exp(big.NewInt(10), big.NewInt(9), nil))
	MaxGasPriceToAdjust = big.NewInt(0).Exp(big.NewInt(10), big.NewInt(11), nil)
)

var logger = plog.NewBizLogger("ethereum")

func NewClient(url string) {
	_client, err := ethclient.Dial(url)

	if err != nil {
		logger.Fatal().Msg("cannot connect to ethereum")
		panic(err)
	}

	_chainId, err := _client.NetworkID(context.Background())
	if err != nil {
		logger.Fatal().Msg("cannot get chainid")
		panic(err)
	}

	logger.Info().Msg("ethereum: start ethereum service suucess")
	chainId = _chainId
	client = _client
}

func Close() {
	client.Close()
}

func GetEthClient() *ethclient.Client {
	return client
}

func GetChainID() *big.Int {
	return chainId
}

func GetBalance(addr string) (*big.Int, error) {
	balance, err := client.BalanceAt(context.TODO(), common.HexToAddress(addr), nil)
	if err != nil {
		logger.Error().Err(err).Var("address", addr).Msg("error when get balance")
	}
	return balance, err
}

func GetBalanceDecimal(addr string, decimals int64) (float64, error) {
	balance, err := GetBalance(addr)
	if err != nil {
		return 0, err
	}
	inFloat := ethutils.ToDecimal(balance, decimals)
	return inFloat, nil
}

func GetTransactionReceiptStatus(txHash string) (int64, *types.Transaction, error) {
	startTime := time.Now()
	i := 1
	var trans *types.Transaction
	for true {
		time.Sleep(time.Duration(2*i+1) * time.Second) // todo sleep pattern & timeout
		_trans, isPending, err := client.TransactionByHash(context.TODO(), common.HexToHash(txHash))
		if err != nil {
			return 0, trans, err
		}
		trans = _trans
		if !isPending {
			break
		}
		i++
		if time.Now().Sub(startTime) > time.Duration(30)*time.Minute {
			return 0, trans, fmt.Errorf("get TransactionByHash timeout")
		}
	}
	status, err := getTransactionReceiptStatus(txHash)
	return status, trans, err
}

func getTransactionReceiptStatus(txHash string) (int64, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(20)*time.Second)
	defer cancelFunc()
	receipt, err := client.TransactionReceipt(ctx, common.HexToHash(txHash))
	if err != nil {
		logger.Error().Err(err).Msg("error when execute transaction")
		return 0, err
	}
	return int64(receipt.Status), nil
}
