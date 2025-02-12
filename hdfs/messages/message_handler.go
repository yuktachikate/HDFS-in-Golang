package messages

import (
	"encoding/binary"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}

	return m
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.conn.Write(prefix)
	m.conn.Write(serialized)

	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.conn.Read(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	//m.conn.Read(payload)

	total := uint64(0)
	for total < payloadSize {
		n, _ := m.conn.Read(payload[total:])
		total += uint64(n)
		//log.Println(payloadSize, n, err)
	}

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

//func (m *MessageHandler) Receive() (*Wrapper, error) {
//	prefix := make([]byte, 8)
//	m.conn.Read(prefix)
//
//	payloadSize := binary.LittleEndian.Uint64(prefix)
//	payload := make([]byte, payloadSize)
//	m.conn.Read(payload)
//
//	wrapper := &Wrapper{}
//	err := proto.Unmarshal(payload, wrapper)
//	return wrapper, err
//}

func (m *MessageHandler) Close() {
	m.conn.Close()
}

func (m *MessageHandler) GetConnection() net.Conn {
	return m.conn
}

// GetConnection /* get file connection */
func GetConnection(hostname string, port string) (*MessageHandler, error) {
	//establish connection
	conn, err := net.Dial("tcp", hostname+":"+port)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	//defer conn.Close()

	msgHandler := NewMessageHandler(conn)
	return msgHandler, nil
}
