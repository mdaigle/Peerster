package main

import (
	"flag"
	"net"
	"fmt"
	"strconv"
	"github.com/mdaigle/Peerster/part1/protocol"
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
	my_addr, err := net.ResolveUDPAddr("udp4", ":" + strconv.Itoa(client_addr.Port + 1))

	message := protocol.SimpleMessage{SenderName:"N/A", RelayPeer:"N/A", Body:msg_text}
	bytes,_ := protocol.Encode(&message)

	//decoded,_ := protocol.Decode(bytes)
	//print(decoded.Body, decoded.RelayPeer, decoded.SenderName)

	conn,err := net.DialUDP("udp4", my_addr, client_addr)
	conn.Write(bytes)
}