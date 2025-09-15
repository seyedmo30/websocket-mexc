package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	pb "test/proto"

	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
)

type MexcWsClient struct {
	url           string
	proxyAddr     string
	dialer        *websocket.Dialer
	conn          *websocket.Conn
	subscriptions []string
	messageChan   chan *pb.PushDataV3ApiWrapper
	done          chan struct{}
	mutex         sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	missedPings   int
}

type WSEndpoint int

const (
	LimitDepth WSEndpoint = iota
	AggreDeals
)

// Map websockets to URL templates (with %s for symbol)
var endpointTemplates = map[WSEndpoint]string{
	LimitDepth: "spot@public.limit.depth.v3.api.pb@%s@5",
	AggreDeals: "spot@public.aggre.deals.v3.api.pb@100ms@%s",
}

func NewMexcWsClient(url, proxyAddr, symbol string, endpoints ...WSEndpoint) *MexcWsClient {
	socksDialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Fatalf("Failed to create SOCKS5 dialer: %v", err)
	}

	dialer := &websocket.Dialer{
		NetDial: socksDialer.Dial,
	}

	ctx, cancel := context.WithCancel(context.Background())

	subs := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		subs = append(subs, fmt.Sprintf(endpointTemplates[ep], symbol))
	}

	return &MexcWsClient{
		url:           url,
		proxyAddr:     proxyAddr,
		dialer:        dialer,
		subscriptions: subs,
		messageChan:   make(chan *pb.PushDataV3ApiWrapper, 100), // Buffered to avoid blocking
		done:          make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		missedPings:   0,
	}
}

func (client *MexcWsClient) subscribe() {
	if client.conn != nil {
		subMsg := map[string]interface{}{
			"method": "SUBSCRIPTION",
			"params": client.subscriptions,
		}
		if err := client.conn.WriteJSON(subMsg); err != nil {
			log.Printf("Failed to send subscription: %v", err)
		}
	}
}

func (client *MexcWsClient) connect() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	c, _, err := client.dialer.Dial(client.url, nil)
	if err != nil {
		return err
	}

	client.conn = c
	client.missedPings = 0
	client.subscribe()

	return nil
}

func (client *MexcWsClient) Start() {
	go client.run()
}

func (client *MexcWsClient) run() {
	for {
		select {
		case <-client.ctx.Done():
			return
		default:
		}

		if err := client.connect(); err != nil {
			log.Printf("Connection failed: %v, retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Start ping in a separate goroutine
		go client.ping()

		// Start read loop
		client.readLoop()
	}
}

func (client *MexcWsClient) ping() {
	ticker := time.NewTicker(30 * time.Second) // Optimized to 30 seconds as per docs
	defer ticker.Stop()

	for {
		select {
		case <-client.ctx.Done():
			return
		case <-ticker.C:
			client.mutex.Lock()
			if client.conn == nil {
				client.mutex.Unlock()
				return
			}
			err := client.conn.WriteJSON(map[string]string{"method": "PING"})
			if err == nil {
				client.missedPings++
				if client.missedPings > 2 {
					log.Printf("Missed too many pongs (%d), reconnecting...", client.missedPings)
					client.conn.Close()
					client.conn = nil
				}
			} else {
				log.Printf("Failed to send ping: %v", err)
				client.conn.Close()
				client.conn = nil
			}
			client.mutex.Unlock()
		}
	}
}

func (client *MexcWsClient) readLoop() {
	for {
		select {
		case <-client.ctx.Done():
			return
		default:
		}

		client.mutex.Lock()
		if client.conn == nil {
			client.mutex.Unlock()
			return
		}
		typeMsg, message, err := client.conn.ReadMessage()
		client.mutex.Unlock()

		if err != nil {
			log.Printf("Read error: %v", err)
			client.mutex.Lock()
			if client.conn != nil {
				client.conn.Close()
				client.conn = nil
			}
			client.mutex.Unlock()
			// Reconnection handled in run loop
			return
		}

		if typeMsg == websocket.BinaryMessage {
			var data pb.PushDataV3ApiWrapper // Use the generated type
			if err := proto.Unmarshal(message, &data); err != nil {
				log.Printf("Unmarshal error: %v", err)
				continue
			}
			client.messageChan <- &data
		} else if typeMsg == websocket.TextMessage {
			var resp map[string]interface{}
			if err := json.Unmarshal(message, &resp); err == nil {
				if msg, ok := resp["msg"].(string); ok {
					if msg == "PONG" {
						client.mutex.Lock()
						client.missedPings = 0
						client.mutex.Unlock()
					} else if msg == "PING" { // Handle potential server ping
						client.conn.WriteJSON(map[string]string{"method": "PONG"})
					}
					// For subscription responses
					if code, ok := resp["code"].(float64); ok && code != 0 {
						log.Printf("Subscription error: %+v", resp)
					}
				}
			}
			fmt.Printf("%d, Received text: %s\n", typeMsg, message)
		}
	}
}

func (client *MexcWsClient) Stop() {
	client.cancel()

	client.mutex.Lock()
	if client.conn != nil {
		// Unsubscribe all
		for _, param := range client.subscriptions {
			unsubMsg := map[string]interface{}{
				"method": "UNSUBSCRIPTION",
				"params": []string{param},
			}
			client.conn.WriteJSON(unsubMsg)
		}
		client.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		client.conn.Close()
		client.conn = nil
	}
	client.mutex.Unlock()

	close(client.messageChan)
}

func main() {
	symbol := "BTCUSDT"
	wsURL := "wss://wbs-api.mexc.com/ws" // Updated endpoint for API/order book access
	proxyAddr := "127.0.0.1:1080"

	client := NewMexcWsClient(wsURL, proxyAddr, symbol, LimitDepth, AggreDeals) // Select endpoints to subscribe to

	client.Start()

	// Handle interrupt for clean shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Process messages
	for {
		select {
		case data, ok := <-client.messageChan:
			if !ok {
				log.Println("Message channel closed")
				return
			}

			switch data.Channel {
			case fmt.Sprintf(endpointTemplates[AggreDeals], symbol):
				fmt.Println("============================start")
				fmt.Printf("Unmarshaled data: %+v\n", data)
				agg := data.GetPublicAggreDeals()
				if agg != nil {
					fmt.Printf("agg.Deals: %+v\n", len(agg.Deals))
					for _, deal := range agg.Deals {
						fmt.Printf("deal: %+v\n", deal)
					}
				} else {
					fmt.Println("agg is nil")
				}
				fmt.Println("============================finish")

			case fmt.Sprintf(endpointTemplates[LimitDepth], symbol):
				fmt.Println("============================start")
				fmt.Printf("Unmarshaled data: %+v\n", data)
				publicLimitDepths := data.GetPublicLimitDepths()
				if publicLimitDepths != nil {
					fmt.Printf("publicLimitDepths.Asks: %+v\n", publicLimitDepths.Asks)
					fmt.Printf("publicLimitDepths.Bids: %+v\n", publicLimitDepths.Bids)
				} else {
					fmt.Println("depth is nil")
				}
				fmt.Println("============================finish")

			}
		case <-interrupt:
			log.Println("Interrupt received, stopping client...")
			client.Stop()
			return
		}
	}
}
