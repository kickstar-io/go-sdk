package ristretto

import (
	"fmt"
	"log"
	"testing"
)

func TestNew(t *testing.T) {

	h := GetInc()
	m := GetInc()
	fmt.Println(h, m)
}

func TestAddKey(t *testing.T) {
	h := GetInc()

	a := "1123123"
	err := h.Set("1234565432", a)
	if err != nil {
		return
	}

	var b string
	_, err = h.Get("1234565432", &b)
	if err != nil {
		return
	}

	log.Println(a, b)
	//assert.Equal(t, a, b)
}
