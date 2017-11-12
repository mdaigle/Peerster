package main

import (
	"flag"
	"net"
	"fmt"
	"github.com/mdaigle/Peerster/routing/protocol"
)

// ./client   -UIPort=10001   -msg=Hello
func main() {
	var ui_port string
	var msg_text string
	var dest string

	flag.StringVar(&ui_port, "UIPort", "10001", "an int")
	flag.StringVar(&msg_text, "msg", "", "a string")
	flag.StringVar(&dest, "Dest", "", "a string")
	flag.Parse()

	client_addr, err := net.ResolveUDPAddr("udp4", ":"+ui_port)
	if err != nil {
		fmt.Println("error resolving client address " + ui_port)
	}

	// send the message from the next port
	my_addr, err := net.ResolveUDPAddr("udp4", "")

	peer_message := protocol.PeerMessage{ID:0,Text:msg_text}
	message := protocol.GossipPacket{}
	if dest == "" {
		message = protocol.GossipPacket{
			Rumor: &protocol.RumorMessage{
				PeerMessage: peer_message,
			},
		}
	} else {
		message = protocol.GossipPacket{
			Private: &protocol.PrivateMessage{
				Dest: dest,
				PeerMessage: peer_message,
			},
		}
	}

	//decoded,_ := protocol.Decode(bytes)
	//print(decoded.Body, decoded.RelayPeer, decoded.SenderName)
	bytes, _ := protocol.Encode(&message)
	conn,err := net.DialUDP("udp4", my_addr, client_addr)
	conn.Write(bytes)
}