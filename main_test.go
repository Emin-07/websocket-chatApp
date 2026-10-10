package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const host = "ws://localhost"

type TestConfig struct {
	clientCount    int
	wg             *sync.WaitGroup
	brMsgCount     *atomic.Int64
	targetMsgCount int
	sendsDone      *atomic.Bool
}

type TestClient struct {
	conn  *websocket.Conn
	msgCh chan *ReqMsg
	done  chan struct{}
}

func NewTestClient(conn *websocket.Conn) *TestClient {
	return &TestClient{
		conn:  conn,
		msgCh: make(chan *ReqMsg, 64),
		done:  make(chan struct{}),
	}
}

func (tc *TestClient) writeLoop() {
	for {
		select {
		case <-tc.done:
			return
		case msg := <-tc.msgCh:
			time.Sleep(time.Millisecond * 100)
			err := tc.conn.WriteJSON(msg)
			if err != nil {
				fmt.Printf("error writing message: %v\n", err)
				return
			}
		}
	}
}

func DialServer(tc *TestConfig) *websocket.Conn {
	exit := make(chan struct{})
	dialer := websocket.Dialer{}

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSport), nil)
	if err != nil {
		log.Fatalf("Error on connection: %s\n", err)
	}

	go func() {
		for {
			time.Sleep(time.Second * 2)
			if tc.sendsDone.Load() && tc.targetMsgCount >= int(tc.brMsgCount.Load()) {
				exit <- struct{}{}
				return
			}
		}
	}()

	go func() {
		<-exit
		conn.Close()
		tc.wg.Done()
	}()

	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) &&
					!strings.Contains(err.Error(), "use of closed network connection") {
					fmt.Printf("error reading a message: %v\n", err)
				}
				return
			}
			if len(b) > 0 {
				tc.brMsgCount.Add(1)
			}
		}
	}()
	return conn
}

func TestConnection(t *testing.T) {
	go CreateWSServer()
	time.Sleep(time.Millisecond * 100)

	clientCount := 5
	brCount := 3
	target := clientCount * brCount

	tc := &TestConfig{clientCount: clientCount, wg: &sync.WaitGroup{},
		brMsgCount: &atomic.Int64{}, targetMsgCount: target, sendsDone: &atomic.Bool{}}

	tc.sendsDone.Store(false)

	tc.wg.Add(clientCount + 1)

	brConn := DialServer(tc)
	brClient := NewTestClient(brConn)

	go brClient.writeLoop()

	for range clientCount {
		go DialServer(tc)
	}

	time.Sleep(time.Second)

	for range brCount {
		msg := &ReqMsg{
			MsgType: BroadCastMsg,
			Data:    "Hello from tests",
		}

		brClient.msgCh <- msg
	}
	tc.sendsDone.Swap(true)

	tc.wg.Wait()
	brClient.done <- struct{}{}

	time.Sleep(time.Second)
	fmt.Println("exiting test")
}
