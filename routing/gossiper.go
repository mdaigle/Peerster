package main

import (
	"net"
	"flag"
	"fmt"
	"strings"
	"github.com/mdaigle/Peerster/part3/protocol"
	"sync"
	"math/rand"
	"time"
	"github.com/gorilla/mux"
	"net/http"
	"log"
	"encoding/json"
	"os"
	"github.com/gorilla/handlers"
)

var gossip_addr *net.UDPAddr
var name string
var local_id uint32

// Set of known peers
var peers_map map[string]bool
var peers_map_lock sync.Mutex
var peers []string

// Routing table for p2p messaging
var next_hop map[string]string
var rtimer int64

var status_vector map[string][]*protocol.GossipPacket
var status_vector_lock sync.Mutex

var new_messages []*protocol.GossipPacket
var new_peers_index int

var client_conn *net.UDPConn
var gossip_conn *net.UDPConn

func init() {
	peers_map = make(map[string]bool)
	next_hop = make(map[string]string)
	status_vector = make(map[string][]*protocol.GossipPacket)
	local_id = 0
	rand.Seed(time.Now().Unix())
	new_messages = make([]*protocol.GossipPacket, 0)
	new_peers_index = 0
}

//./gossiper -UIPort=10000 -gossipPort=127.0.0.1:5000 -name=nodeA -peers_map=127.0.0.1:5001_10.1.1.7:5002
func main() {
	var client_addr *net.UDPAddr
	var ui_port string
	var gossip_port string
	var peers_string string
	var err error

	flag.StringVar(&ui_port, "UIPort", "10000", "an int")
	flag.StringVar(&gossip_port, "gossipPort", "", "an address with port")
	flag.StringVar(&name, "name", "", "a string")
	flag.StringVar(&peers_string, "peers", "", "a comma separated list of addresses with ports")
	flag.Int64Var(&rtimer, "rtimer", 60, "Time between route rumor messages")
	flag.Parse()

	client_addr, err = net.ResolveUDPAddr("udp4", ":"+ui_port)
	if err != nil {
		fmt.Println("error resolving client address " + ui_port)
	}

	gossip_addr, err = net.ResolveUDPAddr("udp4", gossip_port)
	if err != nil {
		fmt.Println("error resolving gossip address " + gossip_port)
	}

	for _,addr := range strings.Split(peers_string, ",") {
		if addr == "" {
			continue
		}
		peer_addr, err := net.ResolveUDPAddr("udp4", addr)
		if err != nil {
			fmt.Println("error resolving peer address " + addr)
		}
		peers_map[peer_addr.String()] = true
		peers = append(peers, addr)
	}

	client_conn, err = net.ListenUDP("udp4", client_addr)
	gossip_conn, err = net.ListenUDP("udp4", gossip_addr)

	go readClient()
	go readGossip()
	go periodicEvents()
	webServer()
}

func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/id", changeId).Methods("POST")
	r.HandleFunc("/node", newNode).Methods("POST")
	r.HandleFunc("/node", serveNodes).Methods("GET")
	r.HandleFunc("/message", sendMessage).Methods("POST")
	r.HandleFunc("/message", serveMessages).Methods("GET")

	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("."))))
	headersOk := handlers.AllowedHeaders([]string{"X-Requested-With"})
	originsOk := handlers.AllowedOrigins([]string{os.Getenv("ORIGIN_ALLOWED"), "null"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})

	log.Fatal(http.ListenAndServe("127.0.0.1:8080", handlers.CORS(originsOk, headersOk, methodsOk)(r)))
}

func changeId(w http.ResponseWriter, r *http.Request) {
	return
	/*vars := mux.Vars(r)
	name = vars["id"]
	local_id = 0
	w.WriteHeader(http.StatusOK)*/
}

func newNode(w http.ResponseWriter, r *http.Request) {
	return
	/*vars := mux.Vars(r)
	peer_addr_string := vars["address"]
	peer_addr, err := net.ResolveUDPAddr("udp4", peer_addr_string)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}
	// Add to peers if new peer
	_, ok := peers_map[peer_addr.String()]
	if !ok {
		peers_map[peer_addr.String()] = true
		peers = append(peers, peer_addr.String())
	}
	peers_json,_ := json.Marshal(peers)
	w.Write(peers_json)*/
}

func serveNodes(w http.ResponseWriter, r * http.Request) {
	if new_peers_index < len(peers) {
		new_peers := peers[new_peers_index:]
		new_peers_index = len(peers)
		json_body, _ :=json.Marshal(new_peers)
		w.Write(json_body)
	}
}

type ClientMessage struct {
	Text string `json:"text"`
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	text := r.PostForm.Get("text")

	message := &protocol.GossipPacket{Rumor: &protocol.RumorMessage{Origin:"client", PeerMessage: protocol.PeerMessage{ID: 0, Text:text}}}
	go sendClientMessage(message)

	w.WriteHeader(http.StatusOK)
}

func serveMessages(w http.ResponseWriter, r *http.Request) {
	json_body,_ := json.Marshal(new_messages)
	w.Write(json_body)
	new_messages = new_messages[:0]
}

func readClient() {
	for {
		_, buf, err := readFromUDPConn(client_conn)
		if err != nil {
			fmt.Println(err)
		}

		message, err := protocol.Decode(*buf)
		if err != nil {
			fmt.Println("Error decoding", err)
			continue
		}
		sendClientMessage(message)
		new_messages = append(new_messages, message)
	}
}

func sendClientMessage(message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	// Update the message fields
	message.Rumor.Origin = name
	local_id++
	message.Rumor.PeerMessage.ID = local_id

	printoutMessageReceived(nil, message, true)

	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		// Add origin to status vector if new
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
	status_vector_lock.Unlock()

	// Forward the message if there are peers
	if len(peers) == 0 {
		return
	}

	// Create a permutation of known peers
	remaining_addrs := make([]string, len(peers))
	copy(remaining_addrs, peers)
	for i := range remaining_addrs {
		j := rand.Intn(i + 1)
		remaining_addrs[i], remaining_addrs[j] = remaining_addrs[j], remaining_addrs[i]
	}

	new_peer_addr_str := remaining_addrs[0]
	new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)
	remaining_addrs = remaining_addrs[1:]
	go startGossiping(new_peer_addr, message)
}

func readGossip() {
	for {
		peer_addr, buf, err := readFromUDPConn(gossip_conn)
		if err != nil {
			fmt.Println(err)
		}

		go processMessage(peer_addr, buf)
	}
}

func periodicEvents(startup bool) {
	anti_entropy := time.NewTicker(time.Second)
	route_rumor := time.NewTicker(time.Duration(rtimer) * time.Second)
	for {
		select {
			case <-anti_entropy.C:
				// pick a random peer
				if len(peers) > 0 {
					peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
					peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
					// send a status message
					sendStatus(peer_addr)
				}
				break
			case startup:
			case <-route_rumor.C:
				// pick a random peer
				if len(peers) > 0 {
					peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
					peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
					// send a status message
					sendRouteRumor(peer_addr)
				}
				break
		}
	}
}

func processMessage(peer_addr *net.UDPAddr, buf *[]byte) {
	message, err := protocol.Decode(*buf)
	if err != nil {
		fmt.Println("Error decoding", err)
		return
	}

	if (message.Rumor != nil && message.Status != nil) ||
		(message.Rumor == nil && message.Status == nil){
		fmt.Println("Malformed Message: rumor and status either both nil or both non-nil")
		return
	}

	// Add to peers if new peer
	peers_map_lock.Lock()
	_, ok := peers_map[peer_addr.String()]
	if !ok {
		peers_map[peer_addr.String()] = true
		peers = append(peers, peer_addr.String())
	}
	peers_map_lock.Unlock()

	printoutMessageReceived(peer_addr, message, false)

	if message.Rumor != nil {
		processRumor(peer_addr, message)
	} else {
		processStatus(peer_addr, message)
	}
}

func processRumor(peer_addr *net.UDPAddr,message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	// Add origin to status vector if new
	messages,ok := status_vector[message.Rumor.Origin]
	if !ok {
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
		messages = status_vector[message.Rumor.Origin]
	}

	// Check if this is the next message we want from the origin
	if message.Rumor.PeerMessage.ID == uint32(len(messages)) {
		// Store the rumor
		status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
		new_messages = append(new_messages, message)

		// Update the next hop routing table
		next_hop[message.Rumor.Origin] = peer_addr.String()

		// Pick a random peer (other than sender) and start gossiping with them
		if len(peers) > 1 {
			new_peer_addr_str := peers[int(rand.Float32() * float32(len(peers)))]
			for {
				if new_peer_addr_str == peer_addr.String() {
					new_peer_addr_str = peers[int(rand.Float32() * float32(len(peers)))]
					continue
				}
				break
			}
			new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)

			go startGossiping(new_peer_addr, message)
		}
	}

	// Send a status as an ACK
	go sendStatus(peer_addr)
}

func processStatus(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	// Compare the status vectors
	for _,peer_status := range message.Status.Want {
		status_vector_lock.Lock()
		messages, ok := status_vector[peer_status.Identifier]
		if !ok {
			// Add origin to status vector if new
			status_vector[peer_status.Identifier] = make([]*protocol.GossipPacket, 1)
			messages = status_vector[peer_status.Identifier]
		}
		status_vector_lock.Unlock()

		if uint32(len(messages)) > peer_status.NextID {
			// We're ahead, so get the peer up-to-date
			// Get message from peer[identifier] with id NextID
			message_to_send := messages[peer_status.NextID]
			message_bytes, _ := protocol.Encode(message_to_send)
			gossip_conn.WriteToUDP(message_bytes, peer_addr)
			return
		} else if uint32(len(messages)) < peer_status.NextID {
			// We're behind, so ask the peer to get us up-to-date
			sendStatus(peer_addr)
			return
		}
	}
	printoutInSync(peer_addr)
}

func sendStatus(addr *net.UDPAddr) {
	peer_statuses := make([]protocol.PeerStatus, 0)
	status_vector_lock.Lock()
	for id,messages := range status_vector {
		next_id := uint32(len(messages))
		peer_statuses = append(peer_statuses, protocol.PeerStatus{Identifier:id, NextID:next_id})
	}
	status_vector_lock.Unlock()
	message := &protocol.GossipPacket{Status: &protocol.StatusPacket{Want:peer_statuses}}

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendRouteRumor(addr *net.UDPAddr) {
	status_vector_lock.Lock()
	message := &protocol.GossipPacket{Rumor: &protocol.RumorMessage{Origin:name, PeerMessage: protocol.PeerMessage{ID: 0, Text:""}}}
	local_id++
	message.Rumor.PeerMessage.ID = local_id

	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		// Add origin to status vector if new
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
	status_vector_lock.Unlock()

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func startGossiping(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	printoutMongering(peer_addr)

	// Send the message
	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, peer_addr)

	// Flip a coin and check if there are new peers to gossip with
	if rand.Float32() >= 0.5 && len(peers) > 1{
		// Pick a new peer to gossip with
		new_peer_addr_str := peers[int(rand.Float32() * float32(len(peers)))]
		for {
			if new_peer_addr_str == peer_addr.String() {
				new_peer_addr_str = peers[int(rand.Float32() * float32(len(peers)))]
				continue
			}
			break
		}
		new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)

		printoutFlipPassed(new_peer_addr)

		// Start gossiping with the new peer
		go startGossiping(new_peer_addr, message)
	}
}

func readFromUDPConn(conn *net.UDPConn) (*net.UDPAddr, *[]byte, error) {
	buf := make([]byte, 1024)
	num_bytes, addr, err := conn.ReadFromUDP(buf[0:])

	if err != nil {
		return addr, nil, err
	}

	buf = buf[0:num_bytes]
	return addr, &buf, nil
}

// Assumes the message is well-formed
func printoutMessageReceived(peer_addr *net.UDPAddr, message *protocol.GossipPacket, client bool) {
	if client {
		fmt.Println("CLIENT", message.Rumor.PeerMessage.Text, message.Rumor.Origin)
	} else {
		if message.Rumor != nil {
			fmt.Println("RUMOR origin", message.Rumor.Origin, "from", peer_addr.String(), "ID", message.Rumor.PeerMessage.ID, "contents", message.Rumor.PeerMessage.Text)
		} else {
			fmt.Print("STATUS from ", peer_addr.String())
			for _,peer_status := range message.Status.Want {
				fmt.Print(" origin ", peer_status.Identifier, " nextID ", peer_status.NextID)
			}
			fmt.Println()
		}
	}

	fmt.Println(strings.Join(peers, ","))
}

func printoutMongering(peer_addr *net.UDPAddr) {
	fmt.Println("MONGERING with", peer_addr.String())
}

func printoutFlipPassed(peer_addr *net.UDPAddr) {
	fmt.Println("FLIPPED COIN sending rumor to", peer_addr.String())
}

func printoutInSync(peer_addr *net.UDPAddr) {
	fmt.Println("IN SYNC WITH", peer_addr.String())
}