package websocket

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

// Hub maintains active websocket connections and broadcasts messages
type Hub struct {
	// Registered clients (map[userID]map[connectionID]*Client)
	clients map[string]map[string]*Client
	mu      sync.RWMutex

	// Broadcast channel
	broadcast chan *Message

	// Register/Unregister channels
	register   chan *Client
	unregister chan *Client
}

// Client represents a websocket connection
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	userID string
	id     string
}

// Message represents a websocket message
type Message struct {
	Type      string                 `json:"type"`
	UserID    string                 `json:"user_id,omitempty"`
	JobID     string                 `json:"job_id,omitempty"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// NewHub creates a new WebSocket hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]map[string]*Client),
		broadcast:  make(chan *Message, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the hub's main loop
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case client := <-h.register:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; !ok {
				h.clients[client.userID] = make(map[string]*Client)
			}
			h.clients[client.userID][client.id] = client
			h.mu.Unlock()
			log.Printf("WebSocket client registered: user=%s, conn=%s", client.userID, client.id)
		case client := <-h.unregister:
			h.mu.Lock()
			if userClients, ok := h.clients[client.userID]; ok {
				if _, ok := userClients[client.id]; ok {
					delete(userClients, client.id)
					close(client.send)
					if len(userClients) == 0 {
						delete(h.clients, client.userID)
					}
				}
			}
			h.mu.Unlock()
			log.Printf("WebSocket client unregistered: user=%s, conn=%s", client.userID, client.id)
		case message := <-h.broadcast:
			// Broadcast to specific user or all users
			h.mu.RLock()
			if message.UserID != "" {
				if userClients, ok := h.clients[message.UserID]; ok {
					for _, client := range userClients {
						select {
						case client.send <- h.marshalMessage(message):
						default:
							close(client.send)
							delete(userClients, client.id)
						}
					}
				}
			} else {
				// Broadcast to all
				for _, userClients := range h.clients {
					for _, client := range userClients {
						select {
						case client.send <- h.marshalMessage(message):
						default:
							close(client.send)
						}
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends a message to user(s)
func (h *Hub) Broadcast(message *Message) {
	message.Timestamp = time.Now()
	h.broadcast <- message
}

// BroadcastJobProgress broadcasts job progress update
func (h *Hub) BroadcastJobProgress(userID, jobID string, progress float32, status string, data map[string]interface{}) {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["progress"] = progress
	data["status"] = status
	h.Broadcast(&Message{
		Type:   "job_progress",
		UserID: userID,
		JobID:  jobID,
		Data:   data,
	})
}

// BroadcastValidationUpdate broadcasts validation completion
func (h *Hub) BroadcastValidationUpdate(userID, validationID string, data map[string]interface{}) {
	h.Broadcast(&Message{
		Type:   "validation_update",
		UserID: userID,
		JobID:  validationID,
		Data:   data,
	})
}

// marshalMessage converts message to JSON
func (h *Hub) marshalMessage(message *Message) []byte {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		return []byte("{}")
	}
	return data
}

// HandleWebSocket handles WebSocket connection upgrade
func (h *Hub) HandleWebSocket() fiber.Handler {
	return websocket.New(func(c *websocket.Conn) {
		// Get user ID from query or locals
		userID := c.Query("user_id")
		if userID == "" {
			if uid := c.Locals("user_id"); uid != nil {
				userID = uid.(string)
			}
		}
		if userID == "" {
			log.Println("WebSocket connection rejected: no user_id")
			c.Close()
			return
		}

		// Create client
		client := &Client{
			hub:    h,
			conn:   c,
			send:   make(chan []byte, 256),
			userID: userID,
			id:     generateID(),
		}

		// Register client
		h.register <- client

		// Start goroutines
		go client.writePump()
		client.readPump()
	})
}

// readPump reads messages from the websocket connection
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}
		// Handle incoming messages (ping, subscribe, etc.)
		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err == nil {
			if msgType, ok := msg["type"].(string); ok && msgType == "ping" {
				c.send <- []byte(`{"type":"pong"}`)
			}
		}
	}
}

// writePump writes messages to the websocket connection
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// generateID generates a unique connection ID
func generateID() string {
	return time.Now().Format("20060102150405.000000")
}

// GetConnectedUsers returns number of connected users
func (h *Hub) GetConnectedUsers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// GetUserConnections returns number of connections for a user
func (h *Hub) GetUserConnections(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if userClients, ok := h.clients[userID]; ok {
		return len(userClients)
	}
	return 0
}
