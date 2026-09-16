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

func DialServer() {
	dialer := websocket.Dialer{}

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSport), nil)
	if err != nil {
		log.Fatalf("Error on connection: %s\n", err)
	}
	defer conn.Close()

	time.Sleep(time.Second)
}

func TestConnection(t *testing.T) {
	go CreateWSServer()
	time.Sleep(time.Second)

	tc := TestConfig{clientCount: 5, wg: &sync.WaitGroup{}}

	for range tc.clientCount {
		tc.wg.Go(DialServer)
	}

	tc.wg.Wait()
	fmt.Println("exiting test")
}

type MyError struct{ msg string }

func (e *MyError) Error() string { return e.msg }

func doSomething() error {
	var err *MyError
	// ...
	if err != nil {
		return err
	}
	return nil // explicit, not a typed nil in disguise
}

func doer() {
	err := doSomething()
	if err != nil {
		log.Println("failed:", err) // this runs, even though err "is nil"
	}
}
