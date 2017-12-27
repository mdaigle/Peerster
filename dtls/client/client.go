package main

import (
	"flag"
	"net"
	"fmt"
	"github.com/mdaigle/Peerster/filesearch/protocol"
	"encoding/hex"
	"strings"
)

// ./client   -UIPort=10001   -msg=Hello
func main() {
	var ui_port string
	var msg_text string
	var dest string
	var file string
	var request string
	var keyword_str string
	var budget uint64

	flag.StringVar(&ui_port, "UIPort", "10001", "an int")
	flag.StringVar(&msg_text, "msg", "", "a string")
	flag.StringVar(&dest, "Dest", "", "a string")
	flag.StringVar(&file, "file", "", "a filename")
	flag.StringVar(&request, "request", "", "file's metahash")
	flag.StringVar(&keyword_str, "keywords", "", "a list of keywords")
	flag.Uint64Var(&budget, "budget", 2, "uint64 search budget")
	flag.Parse()

	client_addr, err := net.ResolveUDPAddr("udp4", ":"+ui_port)
	if err != nil {
		fmt.Println("error resolving client address " + ui_port)
	}

	// send the message from the next port
	my_addr, err := net.ResolveUDPAddr("udp4", "")

	var message protocol.GossipPacket
	if msg_text != "" {
		peer_message := protocol.PeerMessage{ID: 0, Text: msg_text}
		if dest == "" {
			message = protocol.GossipPacket{
				Rumor: &protocol.RumorMessage{
					PeerMessage: peer_message,
				},
			}
		} else {
			message = protocol.GossipPacket{
				Private: &protocol.PrivateMessage{
					Dest:        dest,
					PeerMessage: peer_message,
				},
			}
		}
	} else if file != "" && request != "" {
		hash,_ := hex.DecodeString(request)
		message = protocol.GossipPacket{
			DataRequest: &protocol.DataRequest{
				Destination: dest,
				HashValue: hash,
				FileName: file,
			},
		}
	} else if keyword_str != "" {
		message = protocol.GossipPacket{
			SearchRequest: &protocol.SearchRequest{
				Origin: "client",
				Budget: budget,
				Keywords: strings.Split(keyword_str, ","),
			},
		}
	}

	//decoded,_ := protocol.Decode(bytes)
	//print(decoded.Body, decoded.RelayPeer, decoded.SenderName)
	bytes, _ := protocol.Encode(&message)
	conn,err := net.DialUDP("udp4", my_addr, client_addr)
	conn.Write(bytes)
}