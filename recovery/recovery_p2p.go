package recovery

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	peer "github.com/libp2p/go-libp2p-peer"
	ythost "github.com/yottachain/YTDataNode/host"
	"github.com/yottachain/YTDataNode/message"
)

// P2PHostMock mocks a p2p network for test
// type P2PHostMock struct {
// 	NodeID   peer.ID
// 	Data     map[common.Hash][]byte
// 	Peers    map[peer.ID]ythost.Host
// 	RegFuncs map[string]ythost.
// 	lock     sync.Mutex
// }

const defaultNetworkDelay time.Duration = 50

const dataRetrieveMsgType string = "data"

// P2PNetwork network mock
type P2PNetwork map[peer.ID]ythost.Host

//global network mock
var p2pnetwork = P2PNetwork{}

// P2PLocation peerstore.PeerInfo
type P2PLocation struct {
	ID    peer.ID
	Addrs []string
}

// P2PNetworkHelper helper for retrieve data
type P2PNetworkHelper struct {
	self ythost.Host
}

func (p2pHelper *P2PNetworkHelper) RetrieveData(p2pNodeInfo P2PLocation, dataHash []byte) ([]byte, error) {
	err := p2pHelper.self.ConnectAddrStrings(p2pNodeInfo.ID.Pretty(), p2pNodeInfo.Addrs)
	var msg message.DownloadShardRequest
	msg.VHF = dataHash
	if err != nil {
		return nil, err
	}
	msgData, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}

	stm, err := p2pHelper.self.NewMsgStream(context.Background(), p2pNodeInfo.ID.Pretty(), "/node/0.0.1")
	if err != nil {
		return nil, err
	}
	// proto.Marshal(data, &msg)
	resData, err := stm.SendMsgGetResponse(msgData)
	if err != nil {
		return nil, err
	}
	var res message.DownloadShardResponse
	proto.Unmarshal(resData[2:], &res)
	return res.Data, nil
}

// InititalP2PHostMock initializes P2P mock module
// func InititalP2PHostMock(locs []P2PLocation, hashs []common.Hash, dataBlks [][]byte, networkParams ...time.Duration) error {
// 	for i, hash := range hashs {
// 		node := &P2PHostMock{
// 			locs[i].ID,
// 			map[common.Hash][]byte{hash: dataBlks[i]},
// 			map[peer.ID]ythost.Host{},
// 			map[string]ythost.MsgHandlerFunc{},
// 			sync.Mutex{},
// 		}
// 		node.RegisterHandler("ping", func(msg ythost.Msg) []byte {
// 			buf := fmt.Sprintf("messages from %s", node.NodeID)
// 			return []byte(buf)
// 		})

// 		delay := time.Duration(0)
// 		if i < len(networkParams) {
// 			delay = networkParams[i]
// 		} else {
// 			delay = defaultNetworkDelay
// 		}
// 		node.RegisterHandler(dataRetrieveMsgType, func(msg ythost.Msg) []byte {
// 			time.Sleep(delay * time.Millisecond)
// 			datahash := common.BytesToHash(msg.Content)
// 			return node.Data[datahash]
// 		})
// 		p2pnetwork[locs[i].ID] = node
// 	}

// 	return nil
// }

// func (mockP2PNode *P2PHostMock) ID() peer.ID {
// 	return mockP2PNode.NodeID
// }

// func (mockP2PNode *P2PHostMock) Addrs() []string {
// 	return []string{}
// }

// func (mockP2PNode *P2PHostMock) Peerstore() peerstore.Peerstore {
// 	return nil
// }

// func (mockP2PNode *P2PHostMock) Connect(id peer.ID, addrs []string) error {
// 	mockP2PNode.lock.Lock()
// 	defer mockP2PNode.lock.Unlock()
// 	mockP2PNode.Peers[id] = p2pnetwork[id]
// 	return nil
// }

// func (mockP2PNode *P2PHostMock) DisConnect(id peer.ID) error {
// 	mockP2PNode.lock.Lock()
// 	defer mockP2PNode.lock.Unlock()
// 	delete(mockP2PNode.Peers, id)
// 	return nil
// }

// func (mockP2PNode *P2PHostMock) SendMsg(nodeID peer.ID, msgType string, msg ythost.MsgData) ([]byte, error) {
// 	if node, ok := mockP2PNode.Peers[nodeID]; ok {
// 		mockNode := node.(*P2PHostMock)
// 		if regFunc, reg := mockNode.RegFuncs[msgType]; reg {
// 			funcMsg := ythost.Msg{}
// 			funcMsg.MsgType = msgType
// 			funcMsg.Content = msg
// 			resp := regFunc(funcMsg)
// 			return resp, nil
// 		}
// 	}

// 	return nil, fmt.Errorf("ID %v not connected or func %v not regisitered", nodeID, msgType)
// }

// func (mockP2PNode *P2PHostMock) NewStream(id peer.ID, msgType string) (p2pnet.Stream, error) {
// 	return nil, nil
// }

// func (mockP2PNode *P2PHostMock) RegisterHandler(msgType string, massageHandler ythost.MsgHandlerFunc) {
// 	mockP2PNode.lock.Lock()
// 	defer mockP2PNode.lock.Unlock()
// 	mockP2PNode.RegFuncs[msgType] = massageHandler
// }

// func (mockP2PNode *P2PHostMock) UnregisterHandler(msgType string) {
// 	mockP2PNode.lock.Lock()
// 	defer mockP2PNode.lock.Unlock()
// 	delete(mockP2PNode.RegFuncs, msgType)
// }

// func (mockP2PNode *P2PHostMock) Close() {
// 	mockP2PNode.Data = nil
// 	mockP2PNode.Peers = nil
// 	mockP2PNode.RegFuncs = nil
// }

// func initailP2PMockWithShards(hashs []common.Hash, shards [][]byte, delays ...time.Duration) (P2PNetwork, []P2PLocation) {
// 	locations := make([]P2PLocation, len(hashs))
// 	for i := 0; i < len(hashs); i++ {
// 		privateKey, _, _ := p2pcrypt.GenerateKeyPair(p2pcrypt.Secp256k1, 256)
// 		id, err := peer.IDFromPrivateKey(privateKey)
// 		if err != nil {
// 			panic(err)
// 		}
// 		locations[i] = P2PLocation{id, nil}
// 	}

// 	InititalP2PHostMock(locations, hashs, shards, delays...)
// 	p2pnetwork["self"] = &P2PHostMock{
// 		"self",
// 		map[common.Hash][]byte{},
// 		map[peer.ID]ythost.Host{},
// 		map[string]ythost.MsgHandlerFunc{},
// 		sync.Mutex{},
// 	}

// 	return p2pnetwork, locations
// }
