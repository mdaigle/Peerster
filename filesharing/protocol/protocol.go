package protocol

import (
	"github.com/dedis/protobuf"
	"net"
)

type GossipPacket struct {
	Rumor       *RumorMessage
	Status      *StatusPacket
	Private     *PrivateMessage
	DataRequest *DataRequest
	DataReply   *DataReply
}

type RumorMessage struct {
	Origin   string
	PeerMessage
	LastIP   *net.IP
	LastPort *int
}

type PrivateMessage struct {
	Origin   string
	PeerMessage
	Dest     string
	HopLimit uint32
}

type   DataRequest   struct {
	Origin      string
	Destination string
	HopLimit    uint32
	FileName    string
	HashValue   []byte
}

type   DataReply   struct {
	Origin      string
	Destination string
	HopLimit    uint32
	FileName    string
	HashValue   []byte
	Data        []byte
}

type PeerMessage struct {
	ID   uint32
	Text string
}

type StatusPacket struct {
	Want []PeerStatus
}

type PeerStatus struct {
	Identifier string
	NextID     uint32
}

func Encode(message *GossipPacket) ([]byte, error) {
	return protobuf.Encode(message)
}

func Decode(buf []byte) (*GossipPacket, error) {
	message := &GossipPacket{}
	err := protobuf.Decode(buf, message)
	return message, err
}
