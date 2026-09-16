package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

const WSport = ":3223"

type Client struct {
	ID   string
	mu   *sync.RWMutex
	conn *websocket.Conn
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		ID:   rand.Text()[:9],
		mu:   &sync.RWMutex{},
		conn: conn,
	}
}

type Server struct {
	clients       map[string]*Client
	mu            *sync.RWMutex
	joinServerCH  chan *Client
	leaveServerCH chan *Client
}

func NewServer() *Server {
	return &Server{
		clients:       make(map[string]*Client),
		mu:            &sync.RWMutex{},
		joinServerCH:  make(chan *Client, 64),
		leaveServerCH: make(chan *Client, 64),
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

}

func (s *Server) AcceptLoop(ctx context.Context) {
	for {
		select {
		case c := <-s.joinServerCH:
			s.joinServer(c)
		case c := <-s.leaveServerCH:
			s.leaveServer(c)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = c
	fmt.Printf("client %s joined the server\n", c.ID)
}

func (s *Server) leaveServer(c *Client) {
	delete(s.clients, c.ID)
	fmt.Printf("client %s left the server\n", c.ID)
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
