package main

import (
	"flag"
	"net"
	"fmt"
	"github.com/mdaigle/Peerster/part2/protocol"
)

// ./client   -UIPort=10000   -msg=Hello
func main() {
	var ui_port string
	var msg_text string

	flag.StringVar(&ui_port, "UIPort", "10000", "an int")
	flag.StringVar(&msg_text, "msg", "", "a string")
	flag.Parse()

	client_addr, err := net.ResolveUDPAddr("udp4", ":"+ui_port)
	if err != nil {
		fmt.Println("error resolving client address " + ui_port)
	}

	// send the message from the next port
	my_addr, err := net.ResolveUDPAddr("udp4", "")

	peer_message := protocol.PeerMessage{ID:0,Text:msg_text}
	message := protocol.GossipPacket{Rumor: &protocol.RumorMessage{Origin:"283765", PeerMessage: &peer_message}}
	bytes,_ := protocol.Encode(&message)
	print(len(bytes))

	//decoded,_ := protocol.Decode(bytes)
	//print(decoded.Body, decoded.RelayPeer, decoded.SenderName)

	conn,err := net.DialUDP("udp4", my_addr, client_addr)
	conn.Write(bytes)
}