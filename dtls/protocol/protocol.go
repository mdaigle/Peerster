package protocol

import (
	"github.com/dedis/protobuf"
	"net"
	"crypto/cipher"
	"crypto/rsa"
)

type SecurePacket struct {
	MessageType int
	Data []byte
	PublicKey *rsa.PublicKey
}

const (
	HELLO = 0
	HELLOX = 1
	FINALX = 2
	FINAL = 3
	DATA = 4
)

type GossipPacket struct {
	Rumor       *RumorMessage
	Status      *StatusPacket
	Private     *PrivateMessage
	DataRequest *DataRequest
	DataReply   *DataReply
	SearchRequest *SearchRequest
	SearchReply *SearchReply
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

type DataRequest struct {
	Origin      string
	Destination string
	HopLimit    uint32
	FileName    string
	HashValue   []byte
}

type DataReply struct {
	Origin      string
	Destination string
	HopLimit    uint32
	FileName    string
	HashValue   []byte
	Data        []byte
}

type SearchRequest struct {
	Origin   string
	Budget   uint64
	Keywords []string
}

type SearchReply struct {
	Origin      string
	Destination string
	HopLimit    uint32
	Results     []*SearchResult
}

type SearchResult struct {
	FileName     string
	MetafileHash []byte
	ChunkMap     []uint64
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

func EncryptPacket(packet *GossipPacket, cipher_block cipher.Block) []byte {
	json_packet,_ := EncodeGossip(packet)
	src := []byte(json_packet)

	cipher_block.Encrypt(src, src)
	return src
}

func EncodeSecure(message *SecurePacket) ([]byte, error) {
	return protobuf.Encode(message)
}

func EncodeGossip(packet *GossipPacket) ([]byte, error) {
	return protobuf.Encode(packet)
}

func DecodeSecure(buf []byte) (*SecurePacket, error) {
	message := &SecurePacket{}
	err := protobuf.Decode(buf, message)
	return message, err
}

func DecodeGossip(buf []byte) (*GossipPacket, error) {
	message := &GossipPacket{}
	err := protobuf.Decode(buf, message)
	return message, err
}
