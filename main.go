package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "test/proto"

	"github.com/gorilla/websocket"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
)

// MexcWsClient manages websocket connection and subscriptions
type MexcWsClient struct {
	url           string
	proxyAddr     string
	dialer        *websocket.Dialer
	conn          *websocket.Conn
	subscriptions map[string]WSEndpoint
	mutex         sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	MissedPings   int
	pongCh        chan struct{}
}

// WSEndpoint enumerates reserved endpoints
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

// NewMexcWsClient creates a client and pre-registers provided endpoints (only once each)
func NewMexcWsClient(url, proxyAddr string) *MexcWsClient {
	socksDialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Fatalf("Failed to create SOCKS5 dialer: %v", err)
	}

	dialer := &websocket.Dialer{
		NetDial: socksDialer.Dial,
	}

	ctx, cancel := context.WithCancel(context.Background())

	subs := make(map[string]WSEndpoint, 0)

	return &MexcWsClient{
		url:           url,
		proxyAddr:     proxyAddr,
		dialer:        dialer,
		subscriptions: subs,
		ctx:           ctx,
		cancel:        cancel,
		MissedPings:   0,
		pongCh:        make(chan struct{}, 1),
	}
}

// AddEndpoint adds a subscription for a specific endpoint and symbol (idempotent)
func (c *MexcWsClient) AddEndpoint(ep WSEndpoint, symbol string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	topic := fmt.Sprintf(endpointTemplates[ep], symbol)
	if _, ok := c.subscriptions[topic]; ok {
		// already subscribed
		return nil
	}

	c.subscriptions[topic] = ep
	// If connection already established, send subscribe immediately
	if c.conn != nil {
		subMsg := map[string]interface{}{
			"method": "SUBSCRIPTION",
			"params": []string{topic},
		}
		if err := c.conn.WriteJSON(subMsg); err != nil {
			return fmt.Errorf("failed to send subscription: %w", err)
		}
	}
	return nil
}

// RemoveEndpoint unsubscribes a previously-added endpoint (idempotent)
func (c *MexcWsClient) RemoveEndpoint(ep WSEndpoint, symbol string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	topic := fmt.Sprintf(endpointTemplates[ep], symbol)
	if _, ok := c.subscriptions[topic]; !ok {
		// not subscribed
		return nil
	}

	delete(c.subscriptions, topic)
	if c.conn != nil {
		unsubMsg := map[string]interface{}{
			"method": "UNSUBSCRIPTION",
			"params": []string{topic},
		}
		if err := c.conn.WriteJSON(unsubMsg); err != nil {
			return fmt.Errorf("failed to send unsubscription: %w", err)
		}
	}
	return nil
}

// subscribe sends all current subscriptions (used on connect)
func (c *MexcWsClient) subscribe() {
	if c.conn != nil && len(c.subscriptions) > 0 {
		params := make([]string, 0, len(c.subscriptions))
		for topic := range c.subscriptions {
			params = append(params, topic)
		}
		subMsg := map[string]interface{}{
			"method": "SUBSCRIPTION",
			"params": params,
		}
		if err := c.conn.WriteJSON(subMsg); err != nil {
			log.Printf("Failed to send subscription: %v", err)
		}
	}
}

func (c *MexcWsClient) connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, _, err := c.dialer.Dial(c.url, nil)
	if err != nil {
		return err
	}

	c.conn = conn
	c.MissedPings = 0
	c.subscribe()
	return nil
}

func (c *MexcWsClient) Start() {
	go c.run()
}

func (c *MexcWsClient) run() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if err := c.connect(); err != nil {
			log.Printf("Connection failed: %v, retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Start ping in a separate goroutine
		go c.ping()

		// Start read loop
		c.readLoop()
	}
}
func (c *MexcWsClient) ping() {
	// Tunable values:
	pingInterval := 10 * time.Second // how often we send PING
	pongTimeout := 5 * time.Second   // how long we wait for a PONG after sending PING
	maxMissed := 2                   // how many consecutive misses before reconnect

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// ensure single point to write
			c.mutex.Lock()
			if c.conn == nil {
				c.mutex.Unlock()
				return
			}
			if err := c.conn.WriteJSON(map[string]string{"method": "PING"}); err != nil {
				// write failed -> force reconnect
				log.Printf("Failed to send ping: %v; closing connection for reconnect", err)
				c.conn.Close()
				c.conn = nil
				c.mutex.Unlock()
				return
			}
			c.mutex.Unlock()

			// Wait for PONG or timeout
			select {
			case <-c.ctx.Done():
				return
			case <-c.pongCh:
				// got pong, reset counter
				c.mutex.Lock()
				c.MissedPings = 0
				c.mutex.Unlock()
			case <-time.After(pongTimeout):
				// timeout waiting for pong
				c.mutex.Lock()
				c.MissedPings++
				missed := c.MissedPings
				c.mutex.Unlock()

				log.Printf("PONG timeout (missed %d)", missed)

				if missed > maxMissed {
					log.Printf("Missed pongs (%d) > %d; closing connection to trigger reconnect", missed, maxMissed)
					c.mutex.Lock()
					if c.conn != nil {
						c.conn.Close()
						c.conn = nil
					}
					c.mutex.Unlock()
					return
				}
			}
		}
	}
}

func (c *MexcWsClient) readLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mutex.Lock()
		if c.conn == nil {
			c.mutex.Unlock()
			return
		}

		typeMsg, message, err := c.conn.ReadMessage()
		c.mutex.Unlock()

		if err != nil {
			log.Printf("Read error: %v", err)
			c.mutex.Lock()
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.mutex.Unlock()
			// Reconnection handled in run loop
			return
		}

		if typeMsg == websocket.BinaryMessage {
			var data pb.PushDataV3ApiWrapper
			if err := proto.Unmarshal(message, &data); err != nil {
				log.Printf("Unmarshal error: %v", err)
				continue
			}
			// route by channel -> endpoint
			c.mutex.Lock()
			ep, ok := c.subscriptions[data.Channel]
			c.mutex.Unlock()
			if !ok {
				// unknown channel - ignore or log
				log.Printf("Received message for unsubscribed/unknown channel: %s", data.Channel)
				continue
			}

			switch ep {
			case AggreDeals:
				agg := data.GetPublicAggreDeals()
				log.Printf("AggreDealsDTO: deals=%d", len(agg.Deals))
				// if agg == nil {
				// 	return nil, errors.New("aggre deals payload nil")
				// }

			case LimitDepth:
				depth := data.GetPublicLimitDepths()
				log.Printf("LimitDepthDTO: asks=%d bids=%d", len(depth.Asks), len(depth.Bids))

				// if depth == nil {
				// 	return nil, errors.New("limit depth payload nil")
				// }

			}

			// deliver typed DTO via message channel by reusing the pb wrapper for now
			// callers can call buildDTO themselves or we could create a separate dto channel
			// c.messageChan <- &data

		} else if typeMsg == websocket.TextMessage {
			var resp map[string]interface{}
			if err := json.Unmarshal(message, &resp); err == nil {
				if msg, ok := resp["msg"].(string); ok {
					if msg == "PONG" {
						// notify the ping goroutine (non-blocking)
						select {
						case c.pongCh <- struct{}{}:
						default:
						}
						c.mutex.Lock()
						c.MissedPings = 0
						c.mutex.Unlock()
					} else if msg == "PING" {
						c.conn.WriteJSON(map[string]string{"method": "PONG"})
					}
					if code, ok := resp["code"].(float64); ok && code != 0 {
						log.Printf("Subscription error: %+v", resp)
					}
				}
			}
			fmt.Printf("%d, Received text: %s\n", typeMsg, message)
		}
	}
}

func (c *MexcWsClient) Stop() {
	c.cancel()

	c.mutex.Lock()
	if c.conn != nil {
		// Unsubscribe all
		for topic := range c.subscriptions {
			unsubMsg := map[string]interface{}{
				"method": "UNSUBSCRIPTION",
				"params": []string{topic},
			}
			c.conn.WriteJSON(unsubMsg)
		}
		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.conn.Close()
		c.conn = nil
	}
	c.mutex.Unlock()

}

func main() {
	symbol := "BTCUSDT"
	wsURL := "wss://wbs-api.mexc.com/ws"
	proxyAddr := "127.0.0.1:1080"

	// Create a cancellable context that is bound to OS signals
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer stop()

	client := NewMexcWsClient(wsURL, proxyAddr)
	client.Start()

	// Example runtime control

	// go func() {

	// 	time.Sleep(5 * time.Second)
	// 	client.AddEndpoint(LimitDepth, symbol)

	// }()

	// go func() {

	// 	time.Sleep(5 * time.Second)
	// 	client.RemoveEndpoint(LimitDepth, symbol)

	// }()


	go func() {


		client.AddEndpoint(LimitDepth, symbol)

	}()

	// client.RemoveEndpoint(AggreDeals, symbol)

	<-ctx.Done() // Block until a termination signal
	log.Println("Shutdown signal received, stopping client...")
	client.Stop()
}
