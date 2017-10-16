package main

import (
	"net"
	"flag"
	"fmt"
	"strings"
	"github.com/mdaigle/Peerster/part2/protocol"
	"sync"
	"math/rand"
	"time"
)

var gossip_addr *net.UDPAddr
var name string
var local_id uint32

// Set of known peers
var peers_map map[string]bool
var peers []string

var status_vector map[string][]*protocol.GossipPacket
var status_vector_lock sync.Mutex

var client_conn *net.UDPConn
var gossip_conn *net.UDPConn

func init() {
	peers_map = make(map[string]bool)
	status_vector = make(map[string][]*protocol.GossipPacket)
	local_id = 0
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
	antiEntropy()
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

		printoutMessageReceived(nil, message, true)

		// Update the message fields
		message.Rumor.Origin = name
		local_id++
		message.Rumor.ID = local_id

		status_vector_lock.Lock()
		_,ok := status_vector[message.Rumor.Origin]
		if !ok {
			// Add origin to status vector if new
			status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
		}
		status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
		status_vector_lock.Unlock()

		// Forward the message
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
		go startGossiping(nil, new_peer_addr, message)
	}
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

func antiEntropy() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			// pick a random peer
			if len(peers) > 0 {
				peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
				peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
				// send a status message
				sendStatus(peer_addr)
			}
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
	_, ok := peers_map[peer_addr.String()]
	if !ok {
		peers_map[peer_addr.String()] = true
		peers = append(peers, peer_addr.String())
	}

	printoutMessageReceived(peer_addr, message, false)

	if message.Rumor != nil {
		processRumor(peer_addr, message)
	} else {
		processStatus(peer_addr, message)
	}
}

func processRumor(peer_addr *net.UDPAddr,message *protocol.GossipPacket) {
	// Check if this is the next message we want from the origin
	status_vector_lock.Lock()
	messages,ok := status_vector[message.Rumor.Origin]
	if !ok {
		// Add origin to status vector if new
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
		messages = status_vector[message.Rumor.Origin]
	}

	if message.Rumor.ID == uint32(len(messages)) {
		// refactor this if overhead causes timeouts due to slower ACK
		status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)

		// Create a permutation of known peers
		remaining_addrs := make([]string, len(peers))
		copy(remaining_addrs, peers)
		// Remove the immediate peer addr
		for i := range remaining_addrs {
			if remaining_addrs[i] == peer_addr.String() {
				remaining_addrs = append(remaining_addrs[:i], remaining_addrs[i+1:]...)
				break
			}
		}
		for i := range remaining_addrs {
			j := rand.Intn(i + 1)
			remaining_addrs[i], remaining_addrs[j] = remaining_addrs[j], remaining_addrs[i]
		}

		if len(remaining_addrs) > 0 {
			new_peer_addr_str := remaining_addrs[0]
			new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)
			remaining_addrs = remaining_addrs[1:]

			// Start gossiping with a random peer
			go startGossiping(remaining_addrs, new_peer_addr, message)
		}
	}
	status_vector_lock.Unlock()
	// Send a status as an ACK
	sendStatus(peer_addr)
}

func processStatus(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	// Compare the status vectors
	for _,peer_status := range message.Status.Want {
		messages, ok := status_vector[peer_status.Identifier]
		if !ok {
			// Add origin to status vector if new
			status_vector[peer_status.Identifier] = make([]*protocol.GossipPacket, 1)
			messages = status_vector[peer_status.Identifier]
		}

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
	peer_statuses := make([]*protocol.PeerStatus, 0)
	for id,messages := range status_vector {
		next_id := uint32(len(messages))
		peer_statuses = append(peer_statuses, &protocol.PeerStatus{Identifier:id, NextID:next_id})
	}
	message := &protocol.GossipPacket{Status: &protocol.StatusPacket{Want:peer_statuses}}

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func startGossiping(remaining_addrs []string, peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	printoutMongering(peer_addr)

	// Send the message
	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, peer_addr)

	// Flip a coin and check if there are new peers to gossip with
	if rand.Float32() >= 0.5 && len(remaining_addrs) > 0{
		// Pick a new peer to gossip with
		new_peer_addr_str := remaining_addrs[0]
		new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)
		remaining_addrs = remaining_addrs[1:]

		printoutFlipPassed(new_peer_addr)

		// Start gossiping with the new peer
		go startGossiping(remaining_addrs, new_peer_addr, message)
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
		fmt.Println("CLIENT", message.Rumor.Text, message.Rumor.Origin)
	} else {
		if message.Rumor != nil {
			fmt.Println("RUMOR origin", message.Rumor.Origin, "from", peer_addr.String(), "ID", message.Rumor.ID, "contents", message.Rumor.Text)
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