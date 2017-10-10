package main

import (
	"net"
	"fmt"
	"flag"
	"strings"
	"github.com/mdaigle/Peerster/part1/protocol"
)

var gossip_addr *net.UDPAddr
var name string
var peers_map map[string]bool
var peers []string

func init() {
	peers_map = make(map[string]bool)
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

	client_conn, err := net.ListenUDP("udp4", client_addr)
	gossip_conn, err := net.ListenUDP("udp4", gossip_addr)

	go readClient(client_conn)
	for {
		readGossip(gossip_conn)
	}
}

func readClient(conn *net.UDPConn) {
	for {
		message, err := readFromUDPConn(conn)
		if err != nil {
			fmt.Println(err)
		}

		printout(message, true)

		// Update the message fields
		message.RelayPeer = gossip_addr.String()
		message.SenderName = name

		// Forward the message
		message_bytes, _ := protocol.Encode(&message)
		for _,addr := range peers {
			udp_addr, _ := net.ResolveUDPAddr("udp4", addr)
			conn.WriteToUDP(message_bytes, udp_addr)
		}
	}
}

func readGossip(conn *net.UDPConn) {
	message, err := readFromUDPConn(conn)
	if err != nil {
		fmt.Println(err)
	}

	// Check that the address is valid
	relay_peer, err := net.ResolveUDPAddr("udp4", message.RelayPeer)
	if err != nil {
		fmt.Println("error resolving relay peer address " + message.RelayPeer)
	}

	// Add to peers if new peer
	_, ok := peers_map[relay_peer.String()]
	if !ok {
		peers_map[relay_peer.String()] = true
		peers = append(peers, relay_peer.String())
	}

	printout(message, false)

	// Update relay_peer field
	message.RelayPeer = gossip_addr.String()

	// Forward the message
	message_bytes, _ := protocol.Encode(&message)
	for _,addr := range peers {
		if addr == relay_peer.String() {
			continue
		}
		udp_addr, _ := net.ResolveUDPAddr("udp4", addr)
		conn.WriteToUDP(message_bytes, udp_addr)
	}
}

func readFromUDPConn(conn *net.UDPConn) (protocol.SimpleMessage, error) {
	buf := make([]byte, 1024)
	// don't think a timeout is needed
	//conn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	num_bytes, _, err := conn.ReadFromUDP(buf[0:])

	if err != nil {
		return protocol.SimpleMessage{}, err
	}

	buf = buf[0:num_bytes]

	return protocol.Decode(buf)
}

func printout(message protocol.SimpleMessage, client bool) {
	if client {
		fmt.Println(message.Body, "N/A", "N/A")
	} else {
		fmt.Println(message.Body, message.SenderName, message.RelayPeer)
	}

	fmt.Println(strings.Join(peers, ","))
}