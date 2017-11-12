package main

import (
	"net"
	"flag"
	"fmt"
	"strings"
	"github.com/mdaigle/Peerster/traversal/protocol"
	"sync"
	"math/rand"
	"time"
	"github.com/gorilla/mux"
	"net/http"
	"log"
	"encoding/json"
	"os"
	"github.com/gorilla/handlers"
	"strconv"
)

// Defaults
var DEFAULT_HOP_LIMIT uint32 = 10
var DEFAULT_RTIMER int64 = 60
var DEFAULT_UI_PORT string = "10000"
var DEFAULT_NO_FORWARD bool = false
var DEFAULT_WEB_PORT string = "8080"

// Local node info
var gossip_addr *net.UDPAddr
var name string
var local_id uint32
var no_forward bool
var web_port string

// Set of known peers
var peers_map map[string]bool
var peers_map_lock sync.Mutex
var peers []string

// Routing table for p2p messaging
var next_hop map[string]string
var next_hop_lock sync.Mutex
var rtimer int64

// Global message status vector
var status_vector map[string][]*protocol.GossipPacket
var status_vector_lock sync.Mutex

// Stores of global and private messages
var global_messages []*protocol.GossipPacket
var private_messages map[string][]*protocol.GossipPacket
var private_messages_lock sync.Mutex

// Client and Gossip UDP connections
var client_conn *net.UDPConn
var gossip_conn *net.UDPConn

func init() {
	peers_map = make(map[string]bool)
	next_hop = make(map[string]string)
	status_vector = make(map[string][]*protocol.GossipPacket)
	local_id = 0
	rand.Seed(time.Now().Unix())
	global_messages = make([]*protocol.GossipPacket, 0)
	private_messages = make(map[string][]*protocol.GossipPacket)
}

//./gossiper -UIPort=10000 -gossipPort=127.0.0.1:5000 -name=nodeA -peers_map=127.0.0.1:5001_10.1.1.7:5002
func main() {
	var client_addr *net.UDPAddr
	var ui_port string
	var gossip_port string
	var peers_string string
	var err error

	flag.StringVar(&ui_port, "UIPort", DEFAULT_UI_PORT, "an int")
	flag.StringVar(&gossip_port, "gossipAddr", "", "an address with port")
	flag.StringVar(&name, "name", "", "a string")
	flag.StringVar(&peers_string, "peers", "", "a comma separated list of addresses with ports")
	flag.Int64Var(&rtimer, "rtimer", DEFAULT_RTIMER, "Time between route rumor messages")
	flag.BoolVar(&no_forward, "noforward", DEFAULT_NO_FORWARD, "a boolean")
	flag.StringVar(&web_port, "webPort", DEFAULT_WEB_PORT, "an int")
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
	r.HandleFunc("/id", serveId).Methods("GET")
	r.HandleFunc("/node", newNode).Methods("POST")
	r.HandleFunc("/node", serveNodes).Methods("GET")
	r.HandleFunc("/peers", servePeers).Methods("GET")
	r.HandleFunc("/message", sendMessage).Methods("POST")
	r.HandleFunc("/message/{num_messages}", serveMessages).Methods("GET")
	r.HandleFunc("/message/{peer}", sendPrivateMessage).Methods("POST")
	r.HandleFunc("/message/{peer}/{num_messages}", servePrivateMessages).Methods("GET")

	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("."))))
	headersOk := handlers.AllowedHeaders([]string{"X-Requested-With"})
	originsOk := handlers.AllowedOrigins([]string{os.Getenv("ORIGIN_ALLOWED"), "null"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})

	log.Fatal(http.ListenAndServe("127.0.0.1:" + web_port, handlers.CORS(originsOk, headersOk, methodsOk)(r)))
}

func changeId(w http.ResponseWriter, r *http.Request) {
	return
	/*vars := mux.Vars(r)
	name = vars["id"]
	local_id = 0
	w.WriteHeader(http.StatusOK)*/
}

func serveId(w http.ResponseWriter, r *http.Request) {
	id_json,_ := json.Marshal(name)
	w.Write(id_json)
}

func newNode(w http.ResponseWriter, r *http.Request) {
	peers_map_lock.Lock()
	defer peers_map_lock.Unlock()

	vars := mux.Vars(r)
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
	w.Write(peers_json)
}

func serveNodes(w http.ResponseWriter, r *http.Request) {
	json_body, _ :=json.Marshal(peers)
	w.Write(json_body)
}

func servePeers(w http.ResponseWriter, r *http.Request) {
	json_body, _ := json.Marshal(next_hop)
	w.Write(json_body)
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	text := r.PostForm.Get("text")

	message := &protocol.GossipPacket{Rumor: &protocol.RumorMessage{Origin:"client", PeerMessage: protocol.PeerMessage{ID: 0, Text:text}}}
	go sendClientMessage(message)

	w.WriteHeader(http.StatusOK)
}

func serveMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	num_messages,_ := strconv.Atoi(vars["num_messages"])

	if len(global_messages) == 0 || num_messages >= len(global_messages){
		w.Write(nil)
		return
	}

	json_body,_ := json.Marshal(global_messages[num_messages:])
	w.Write(json_body)
}

func sendPrivateMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peer := vars["peer"]

	r.ParseForm()
	text := r.PostForm.Get("text")

	go sendPrivate(peer, text)

	w.WriteHeader(http.StatusOK)
}

func servePrivateMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peer_name := vars["peer"]
	num_messages,_ := strconv.Atoi(vars["num_messages"])

	pms,ok := private_messages[peer_name]
	if !ok || len(pms) == 0 || num_messages >= len(pms){
		w.Write(nil)
		return
	}

	json_body,_ := json.Marshal(pms[num_messages:])
	w.Write(json_body)
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
		global_messages = append(global_messages, message)
	}
}

func sendClientMessage(message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	// Update the message fields
	message.Rumor.Origin = name
	local_id++
	message.Rumor.PeerMessage.ID = local_id

	printoutMessageReceived(nil, message, true)

	global_messages = append(global_messages, message)

	// Add origin to status vector if new
	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	// Update the status vector
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)

	// Abort if there are no peers
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

	// Resolve the peer address and start gossiping
	new_peer_addr_str := remaining_addrs[0]
	new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)
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

func periodicEvents() {
	anti_entropy := time.NewTicker(time.Second)
	route_rumor := time.NewTicker(time.Duration(rtimer) * time.Second)

	// We want to send a route rumor upon startup, then based on timer
	if len(peers) > 0 {
		peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
		peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
		// send a status message
		sendRouteRumor(peer_addr)
	}

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

	num_msg_types := 0
	if message.Rumor!=nil {num_msg_types++}
	if message.Status!=nil {num_msg_types++}
	if message.Private!=nil {num_msg_types++}
	if num_msg_types != 1{
		fmt.Println("Malformed Message: more than one component")
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
	} else if message.Status != nil{
		processStatus(peer_addr, message)
	} else {
		processPrivate(message)
	}
}

func processRumor(peer_addr *net.UDPAddr,message *protocol.GossipPacket) {
	// Send a status as an ACK
	defer func() {sendStatus(peer_addr)}()

	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	peers_map_lock.Lock()
	// Add to last ip and port if new
	if message.Rumor.LastIP != nil && message.Rumor.LastPort != nil {
		last_addr := message.Rumor.LastIP.String() + ":" + strconv.Itoa(*message.Rumor.LastPort)
		_, ok := peers_map[last_addr]
		if !ok {
			peers_map[last_addr] = true
			peers = append(peers, last_addr)
		}
	}
	peers_map_lock.Unlock()

	// Add origin to status vector if new
	messages,ok := status_vector[message.Rumor.Origin]
	if !ok {
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
		messages = status_vector[message.Rumor.Origin]
	}

	// Check if this is the next message we want from the origin
	if message.Rumor.PeerMessage.ID != uint32(len(messages)) { return }

	// Store the rumor
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
	// Only make messages with non-empty text client accessible
	if message.Rumor.Text != "" {
		global_messages = append(global_messages, message)
	}

	// Update the next hop routing table
	next_hop[message.Rumor.Origin] = peer_addr.String()

	// Drop if no_forward and a non-empty text
	if no_forward && message.Rumor.Text!="" { return }

	// Pick a random peer (other than sender) and start gossiping with them
	if len(peers) <= 1 { return }

	new_peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
	for {
		if new_peer_addr_str != peer_addr.String() { break }
		new_peer_addr_str = peers[int(rand.Float32()*float32(len(peers)))]
	}
	new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)

	// Update the last ip and port
	message.Rumor.LastIP = &peer_addr.IP
	message.Rumor.LastPort = &peer_addr.Port

	go startGossiping(new_peer_addr, message)
}

func processStatus(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	// Compare the status vectors
	for _,peer_status := range message.Status.Want {
		messages, ok := status_vector[peer_status.Identifier]
		if !ok {
			// Add origin to status vector if new
			status_vector[peer_status.Identifier] = make([]*protocol.GossipPacket, 1)
			messages = status_vector[peer_status.Identifier]
		}

		// If we're ahead, get the peer up-to-date
		// Get message from peer[identifier] with id NextID
		// If we're behind, ask the peer to get us up-to-date
		if uint32(len(messages)) > peer_status.NextID {
			// If we're allowed to forward, send the message to the peer
			if no_forward {continue}

			message_to_send := messages[peer_status.NextID]
			message_bytes, _ := protocol.Encode(message_to_send)
			gossip_conn.WriteToUDP(message_bytes, peer_addr)
			return
		} else if uint32(len(messages)) < peer_status.NextID {
			go sendStatus(peer_addr)
			return
		}
	}
	printoutInSync(peer_addr)
}

func processPrivate(message *protocol.GossipPacket) {
	next_hop_lock.Lock()
	defer next_hop_lock.Unlock()

	// Message is meant for us
	if message.Private.Dest == name {
		private_messages_lock.Lock()
		_,ok := private_messages[message.Private.Origin]
		if !ok {
			// Add origin to private_messages keys if new
			private_messages[message.Private.Origin] = make([]*protocol.GossipPacket, 0)
		}
		private_messages[message.Private.Origin] = append(private_messages[message.Private.Origin], message)
		private_messages_lock.Unlock()
		return
	}

	// Drop the message if we're not allowed to forward and it's not for us
	if no_forward {return}

	// If we don't know where to send the message
	// or it's not allowed another hop, drop it
	next,ok := next_hop[message.Private.Dest]
	if !ok || message.Private.HopLimit == 0 {return}

	// Decrement the hop limit to combat routing loops
	message.Private.HopLimit -= 1

	// Resolve the next hop address and forward the message
	next_addr, _ := net.ResolveUDPAddr("udp4", next)
	message_bytes,_ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, next_addr)
}

func sendStatus(addr *net.UDPAddr) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	peer_statuses := make([]protocol.PeerStatus, 0)
	for id,messages := range status_vector {
		next_id := uint32(len(messages))
		peer_statuses = append(peer_statuses, protocol.PeerStatus{Identifier:id, NextID:next_id})
	}

	message := &protocol.GossipPacket{
		Status: &protocol.StatusPacket{
			Want:peer_statuses,
		},
	}

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendRouteRumor(addr *net.UDPAddr) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	message := &protocol.GossipPacket{
		Rumor: &protocol.RumorMessage{
			Origin:name,
			PeerMessage: protocol.PeerMessage{
				ID: 0,
				Text:"",
			},
		},
	}

	local_id++
	message.Rumor.PeerMessage.ID = local_id

	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		// Add origin to status vector if new
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendPrivate(dest string, text string) {
	if dest=="" || text=="" {return}

	private_messages_lock.Lock()
	next_hop_lock.Lock()
	defer private_messages_lock.Unlock()
	defer next_hop_lock.Unlock()

	// Create message, set hop limit to default
	message := &protocol.GossipPacket{
		Private: &protocol.PrivateMessage{
			Origin:name,
			Dest:dest,
			HopLimit: DEFAULT_HOP_LIMIT,
			PeerMessage: protocol.PeerMessage{
				ID:0,
				Text:text,
			},
		},
	}

	// Abort if no known route
	next,ok := next_hop[dest]
	if !ok {return}

	// Initialize conversation if first message for destination
	_,ok = private_messages[dest]
	if !ok {
		// Add origin to private_messages keys if new
		private_messages[dest] = make([]*protocol.GossipPacket, 0)
	}
	private_messages[dest] = append(private_messages[dest], message)

	// Resolve the next hop address and send the message
	next_addr, _ := net.ResolveUDPAddr("udp4", next)
	message_bytes,_ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, next_addr)
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
		} else if message.Status != nil{
			fmt.Print("STATUS from ", peer_addr.String())
			for _,peer_status := range message.Status.Want {
				fmt.Print(" origin ", peer_status.Identifier, " nextID ", peer_status.NextID)
			}
			fmt.Println()
		} else {
			fmt.Println("PRIVATE origin", message.Private.Origin, "contents", message.Private.Text)
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