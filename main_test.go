package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const host = "ws://localhost"

type TestConfig struct {
	clientCount int
	wg          *sync.WaitGroup
}

func DialServer(tc *TestConfig) {
	dialer := websocket.Dialer{}

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSport), nil)
	if err != nil {
		log.Fatalf("Error on connection: %s\n", err)
	}
	defer func() {
		conn.Close()
		tc.wg.Done()
	}()

}

func TestConnection(t *testing.T) {
	go CreateWSServer()
	clientCount := 5

	tc := TestConfig{clientCount: clientCount, wg: &sync.WaitGroup{}}
	tc.wg.Add(clientCount)
	for range clientCount {
		go func() {
			DialServer(&tc)
		}()
	}

	tc.wg.Wait()
	time.Sleep(time.Second)

	fmt.Println("exiting test")
}
