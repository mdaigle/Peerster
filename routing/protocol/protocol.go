package protocol

import "github.com/dedis/protobuf"

type GossipPacket struct {
	Rumor *RumorMessage
	Status *StatusPacket
	Private *PrivateMessage
}

type RumorMessage struct {
	Origin string
	PeerMessage
}

type PrivateMessage struct {
	Origin string
	PeerMessage
	Dest string
	HopLimit uint32
}

type PeerMessage struct {
	ID uint32
	Text string
}

type StatusPacket struct {
	Want []PeerStatus
}

type PeerStatus struct {
	Identifier string
	NextID uint32
}

func Encode(message *GossipPacket) ([]byte, error) {
	return protobuf.Encode(message)
}

func Decode(buf []byte) (*GossipPacket, error) {
	message := &GossipPacket{}
	err := protobuf.Decode(buf, message)
	return message, err
}
