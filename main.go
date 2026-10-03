package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

const WSport = ":3223"

type MsgType string

const BroadCastMsg MsgType = "broadcast"

type ReqMsg struct {
	MsgType MsgType
	Client  *Client
	Data    string
}
type RespMsg struct {
	MsgType  MsgType
	Data     string
	SenderID string
}

func NewRespMsg(msg *ReqMsg) *RespMsg {
	return &RespMsg{
		MsgType:  msg.MsgType,
		Data:     msg.Data,
		SenderID: msg.Client.ID,
	}
}

type Client struct {
	ID    string
	mu    *sync.RWMutex
	conn  *websocket.Conn
	msgCH chan *RespMsg
	done  chan struct{}
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		ID:    rand.Text()[:9],
		mu:    &sync.RWMutex{},
		conn:  conn,
		msgCH: make(chan *RespMsg, 64),
		done:  make(chan struct{}),
	}
}

func (c *Client) writeMsgLoop() {
	defer c.conn.Close()
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.msgCH:
			if err := c.conn.WriteJSON(msg); err != nil {
				fmt.Printf("error sending msg to clientID = %s\n", c.ID)
				return
			}
		}
	}
}

func (c *Client) readMsgLoop(leaveServerCH chan<- *Client, broadCastCH chan<- *ReqMsg) {
	defer func() {
		close(c.done)
		leaveServerCH <- c
	}()

	for {
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := &ReqMsg{}
		if err = json.Unmarshal(b, &msg); err != nil {
			fmt.Println(err)
			continue
		}

		msg.Client = c

		broadCastCH <- msg
	}
}

type Server struct {
	clients       map[string]*Client
	mu            *sync.RWMutex
	joinServerCH  chan *Client
	leaveServerCH chan *Client
	broadCastCH   chan *ReqMsg
}

func NewServer() *Server {
	return &Server{
		clients:       make(map[string]*Client),
		mu:            &sync.RWMutex{},
		joinServerCH:  make(chan *Client, 64),
		leaveServerCH: make(chan *Client, 64),
		broadCastCH:   make(chan *ReqMsg, 64),
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{ReadBufferSize: 512, WriteBufferSize: 512, CheckOrigin: func(r *http.Request) bool { return true }}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP conn upgrade : %s\n", err)
		return
	}

	client := NewClient(conn)
	s.joinServerCH <- client

	go client.writeMsgLoop()
	go client.readMsgLoop(s.leaveServerCH, s.broadCastCH)

}

func (s *Server) AcceptLoop(ctx context.Context) {
	for {
		select {
		case c := <-s.joinServerCH:
			s.joinServer(c)
		case c := <-s.leaveServerCH:
			s.leaveServer(c)
		case msg := <-s.broadCastCH:
			cls := make(map[string]*Client, len(s.clients)-1)
			for id, client := range s.clients {
				if msg.Client.ID != id {
					cls[id] = client
				}
			}
			go s.broadcast(msg, cls)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.mu.Lock()
	s.clients[c.ID] = c
	s.mu.Unlock()
	fmt.Printf("client %s joined the server\n", c.ID)
}

func (s *Server) leaveServer(c *Client) {
	s.mu.Lock()
	delete(s.clients, c.ID)
	s.mu.Unlock()
	fmt.Printf("client %s left the server\n", c.ID)
}

func (s *Server) broadcast(msg *ReqMsg, cls map[string]*Client) {
	resp := NewRespMsg(msg)
	for _, c := range cls {
		c.msgCH <- resp
	}

	fmt.Println("broadcast was sent")
}

func CreateWSServer() {
	srv := NewServer()
	srvCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go srv.AcceptLoop(srvCtx)
	http.HandleFunc("/", srv.handleWS)

	fmt.Printf("server started at port: %s\n", WSport)
	log.Fatal(http.ListenAndServe(WSport, nil))
}
func main() {
	CreateWSServer()
}
