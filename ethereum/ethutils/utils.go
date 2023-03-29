package ethutils

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	"io"
	"math/big"
	"reflect"
	"regexp"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"golang.org/x/crypto/sha3"
)

// PublicKeyBytesToAddress ...
func PublicKeyBytesToAddress(publicKey []byte) common.Address {
	var buf []byte

	hash := sha3.NewLegacyKeccak256() /**/
	hash.Write(publicKey[1:])         // remove EC prefix 04
	buf = hash.Sum(nil)
	address := buf[12:]

	return common.HexToAddress(hex.EncodeToString(address))
}

// IsValidAddress validate hex address
func IsValidAddress(iaddress interface{}) bool {
	re := regexp.MustCompile("^0x[0-9a-fA-F]{40}$")
	switch v := iaddress.(type) {
	case string:
		return re.MatchString(v)
	case common.Address:
		return re.MatchString(v.Hex())
	default:
		return false
	}
}

func Int64ToBytes(number int64) []byte {
	bigInt := new(big.Int)
	bigInt.SetInt64(number)
	return bigInt.Bytes()
}

// IsZeroAddress validate if it's a 0 address
func IsZeroAddress(iaddress interface{}) bool {
	var address common.Address
	switch v := iaddress.(type) {
	case string:
		address = common.HexToAddress(v)
	case common.Address:
		address = v
	default:
		return false
	}

	zeroAddressBytes := common.FromHex("0x0000000000000000000000000000000000000000")
	addressBytes := address.Bytes()
	return reflect.DeepEqual(addressBytes, zeroAddressBytes)
}

// ToDecimal wei to decimals
func ToDecimal(val *big.Int, decimals int64) float64 {
	mul := decimal.NewFromInt(10).Pow(decimal.NewFromInt(decimals))
	num, _ := decimal.NewFromString(val.String())
	result, _ := num.Div(mul).Float64()
	return result
}

// ToWei decimals to wei
func ToWei(amount float64, decimals int64) *big.Int {
	amountDec := decimal.NewFromFloat(amount)
	mul := decimal.NewFromFloat(float64(10)).Pow(decimal.NewFromFloat(float64(decimals)))
	result := amountDec.Mul(mul)
	wei := big.NewInt(0)
	wei.SetString(result.String(), 10)
	return wei
}

func Float64ToByte(f float64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, f)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}

// CalcGasCost calculate gas cost given gas limit (units) and gas price (wei)
func CalcGasCost(gasLimit uint64, gasPrice *big.Int) *big.Int {
	gasLimitBig := big.NewInt(int64(gasLimit))
	return gasLimitBig.Mul(gasLimitBig, gasPrice)
}

// SigRSV signatures R S V returned as arrays
func SigRSV(isig interface{}) ([32]byte, [32]byte, uint8) {
	var sig []byte
	switch v := isig.(type) {
	case []byte:
		sig = v
	case string:
		sig, _ = hexutil.Decode(v)
	}

	sigstr := common.Bytes2Hex(sig)
	rS := sigstr[0:64]
	sS := sigstr[64:128]
	R := [32]byte{}
	S := [32]byte{}
	copy(R[:], common.FromHex(rS))
	copy(S[:], common.FromHex(sS))
	vStr := sigstr[128:130]
	vI, _ := strconv.Atoi(vStr)
	V := uint8(vI + 27)

	return R, S, V
}

func SignHash(data []byte) []byte {
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(data), data)
	return crypto.Keccak256([]byte(msg))
}

func VerifyMsgFromStr(msg string, signature string, fromAddress string) bool {
	ethAddr := common.HexToAddress(fromAddress)
	ethSign := hexutil.MustDecode(signature)
	return VerifyMsg(msg, ethSign, ethAddr)
}

func VerifyMsg(msg string, signature []byte, fromAddress common.Address) bool {
	//// https://github.com/ethereum/go-ethereum/blob/55599ee95d4151a2502465e0afc7c47bd1acba77/internal/ethapi/api.go#L442
	if len(signature) < 65 {
		return false
	}
	if signature[64] != 27 && signature[64] != 28 {
		return false
	}
	signature[64] -= 27

	msgHash := SignHash([]byte(msg))

	pubKey, err := crypto.SigToPub(msgHash, signature)
	if err != nil {
		return false
	}
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return recoveredAddr == fromAddress
}

func DecryptPrivateKey(walletKey, privateKey string) (string, error) {
	plainKey, err := Decrypt(walletKey, privateKey)
	if err != nil {
		return plainKey, err
	}
	return plainKey, nil
}

// Decrypt from base64 to decrypted string
func Decrypt(keyText string, cryptoText string) (string, error) {
	key := []byte(keyText)
	ciphertext, _ := base64.URLEncoding.DecodeString(cryptoText)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// The IV needs to be unique, but not secure. Therefore it's common to
	// include it at the beginning of the ciphertext.
	if len(ciphertext) < aes.BlockSize {
		err := fmt.Errorf("ciphertext too short")
		return "", err
	}
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)

	// XORKeyStream can work in-place if the two arguments are the same.
	stream.XORKeyStream(ciphertext, ciphertext)
	cipherStr := string(ciphertext)
	return cipherStr, nil
}

func CreateWallet() (string, string, error) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return "", "", err
	}

	privateKeyBytes := crypto.FromECDSA(privateKey)
	privateKeyHex := hexutil.Encode(privateKeyBytes)[2:]

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return "", "", err
	}
	address := crypto.PubkeyToAddress(*publicKeyECDSA).Hex()

	return privateKeyHex, address, nil
}

// Encrypt string to base64 crypto using AES
func Encrypt(keyText, text string) (string, error) {
	key := []byte(keyText)
	plaintext := []byte(text)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// The IV needs to be unique, but not secure. Therefore it's common to
	// include it at the beginning of the ciphertext.
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plaintext)

	// convert to base64
	return base64.URLEncoding.EncodeToString(ciphertext), nil
}
