package protocol

import "github.com/dedis/protobuf"

type SimpleMessage struct{
	SenderName string
	RelayPeer string
	Body string
}

func Encode(message *SimpleMessage) ([]byte, error) {
	return protobuf.Encode(message)
}

func Decode(buf []byte) (SimpleMessage, error) {
	message := SimpleMessage{}
	err := protobuf.Decode(buf, &message)
	return message, err
}
