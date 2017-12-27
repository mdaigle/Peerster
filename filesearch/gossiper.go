package main

import (
	"net"
	"flag"
	"fmt"
	"strings"
	"github.com/mdaigle/Peerster/filesearch/protocol"
	"sync"
	"math/rand"
	"time"
	"github.com/gorilla/mux"
	"net/http"
	"log"
	"encoding/json"
	"os"
	"github.com/gorilla/handlers"
	"strconv"
	"crypto/sha256"
	"io"
	"encoding/hex"
	"bytes"
	"github.com/patrickmn/go-cache"
)

// Defaults
var DEFAULT_HOP_LIMIT uint32 = 10
var DEFAULT_RTIMER int64 = 60
var DEFAULT_UI_PORT string = "10000"
var DEFAULT_NO_FORWARD bool = false
var DEFAULT_WEB_PORT string = "8080"
var DEFAULT_FILE_PATH string = "./hw3/_Downloads/"
var RESEND_TIMEOUT = 5
var SEARCH_RESEND_TIMEOUT = 1
var DEFAULT_START_BUDGET uint64 = 2
var MAX_BUDGET uint64 = 32
var MATCH_THRESHOLD uint8 = 2

// Local node info
var gossip_addr *net.UDPAddr
var name string
var local_id uint32
var no_forward bool
var web_port string

// Set of known peers
var peers_map map[string]bool
var peers_map_lock sync.Mutex
var peers []string

// Routing table for p2p messaging
var next_hop map[string]Hop
var next_hop_lock sync.Mutex
var rtimer int64

// Global message status vector
var status_vector map[string][]*protocol.GossipPacket
var status_vector_lock sync.Mutex

// Stores of global and private messages
var global_messages []*protocol.GossipPacket
var private_messages map[string][]*protocol.GossipPacket
var private_messages_lock sync.Mutex

// Store of local file metadata, maps filename to metahash
var local_files map[string]string
var local_chunks map[string][]byte
var local_files_lock sync.Mutex
var local_chunks_lock sync.Mutex

// Stores search results by file
var remote_files map[string][]*RemoteChunks
var remote_file_lock sync.Mutex

// Stores a list of matches for the most recent search
var matches_map map[string]string
var match_order []string
var matches_lock sync.Mutex

var global_budget uint64 = DEFAULT_START_BUDGET

// Stores non-duplicate search requests received in the last .5 seconds
var request_cache *cache.Cache

// Client and Gossip UDP connections
var client_conn *net.UDPConn
var gossip_conn *net.UDPConn

type Hop struct {
	Id uint32
	Direct bool
	Next string
}

type FileMetadata struct {
	Name string
	Size int64
	Metafile *os.File
	MetaHash []byte
}

type RemoteChunks struct {
	Origin string
	ChunkMap []uint64
	MetafileHash []byte
}

func init() {
	peers_map = make(map[string]bool)
	next_hop = make(map[string]Hop)
	status_vector = make(map[string][]*protocol.GossipPacket)
	local_id = 0
	rand.Seed(time.Now().Unix())
	global_messages = make([]*protocol.GossipPacket, 0)
	private_messages = make(map[string][]*protocol.GossipPacket)
	local_files = make(map[string]string)
	local_chunks = make(map[string][]byte)
	remote_files = make(map[string][]*RemoteChunks)
	matches_map = make(map[string]string)
	match_order = make([]string, 0)
	request_cache = cache.New(500*time.Millisecond, 10*time.Minute)
}

//./gossiper -UIPort=10000 -gossipPort=127.0.0.1:5000 -name=nodeA -peers_map=127.0.0.1:5001_10.1.1.7:5002
func main() {
	var client_addr *net.UDPAddr
	var ui_port string
	var gossip_port string
	var peers_string string
	var err error

	flag.StringVar(&ui_port, "UIPort", DEFAULT_UI_PORT, "an int")
	flag.StringVar(&gossip_port, "gossipAddr", "", "an address with port")
	flag.StringVar(&name, "name", "", "a string")
	flag.StringVar(&peers_string, "peers", "", "a comma separated list of addresses with ports")
	flag.Int64Var(&rtimer, "rtimer", DEFAULT_RTIMER, "Time between route rumor messages")
	flag.BoolVar(&no_forward, "noforward", DEFAULT_NO_FORWARD, "a boolean")
	flag.StringVar(&web_port, "webPort", "", "an int")
	flag.Parse()

	client_addr, err = net.ResolveUDPAddr("udp4", ":"+ui_port)
	if err != nil {
		fmt.Println("error resolving client address " + ui_port)
	}

	gossip_addr, err = net.ResolveUDPAddr("udp4", gossip_port)
	if err != nil {
		fmt.Println("error resolving gossip address " + gossip_port)
	}

	for _,addr := range strings.Split(peers_string, ",") {
		if addr == "" || addr == gossip_addr.String() {
			continue
		}
		peer_addr, err := net.ResolveUDPAddr("udp4", addr)
		if err != nil {
			fmt.Println("error resolving peer address " + addr)
		}
		peers_map[peer_addr.String()] = true
		peers = append(peers, addr)
	}

	client_conn, err = net.ListenUDP("udp4", client_addr)
	if err != nil {log.Fatal(err)}
	defer client_conn.Close()
	//if client_conn == nil {log.Fatal("nil client conn")}
	gossip_conn, err = net.ListenUDP("udp4", gossip_addr)
	if err != nil {log.Fatal(err)}
	defer gossip_conn.Close()
	//if gossip_conn == nil {log.Fatal("nil gossip conn")}

	go readClient()
	go readGossip()
	if web_port != "" {
		go webServer()
	}
	periodicEvents()
}

func webServer() {
	r := mux.NewRouter()
	r.HandleFunc("/file/search", fileSearch).Methods("POST")
	r.HandleFunc("/file/search", servefileSearchResults).Methods("GET")
	r.HandleFunc("/file/upload", fileUpload).Methods("POST")
	r.HandleFunc("/file/download", fileDownload).Methods("POST")
	r.HandleFunc("/file/list/local", fileListLocal).Methods("GET")
	r.HandleFunc("/id/{id}", changeId).Methods("POST")
	r.HandleFunc("/id", serveId).Methods("GET")
	r.HandleFunc("/node", newNode).Methods("POST")
	r.HandleFunc("/node", serveNodes).Methods("GET")
	r.HandleFunc("/peers", servePeers).Methods("GET")
	r.HandleFunc("/message", sendMessage).Methods("POST")
	r.HandleFunc("/message/{num_messages}", serveMessages).Methods("GET")
	r.HandleFunc("/message/{peer}", sendPrivateMessage).Methods("POST")
	r.HandleFunc("/message/{peer}/{num_messages}", servePrivateMessages).Methods("GET")

	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("."))))
	headersOk := handlers.AllowedHeaders([]string{"X-Requested-With"})
	originsOk := handlers.AllowedOrigins([]string{os.Getenv("ORIGIN_ALLOWED"), "null"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})

	log.Fatal(http.ListenAndServe("127.0.0.1:" + web_port, handlers.CORS(originsOk, headersOk, methodsOk)(r)))
}

func fileSearch(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	keyword_string := r.PostForm.Get("keywords")
	keywords := strings.Split(keyword_string, ",")

	budget := DEFAULT_START_BUDGET

	searchForFile(keywords, budget)
}

func searchForFile(keywords []string, budget uint64) {
	global_budget = budget
	num_matches := 0
	// reset remote files
	remote_files = make(map[string][]*RemoteChunks)
	// reset matches
	matches_map = make(map[string]string)
	match_order = make([]string, 0)

	fmt.Println("Initiating search with budget", budget)
	go initiateFileSearch(keywords, budget)

	resend := time.NewTicker(time.Duration(SEARCH_RESEND_TIMEOUT) * time.Second)

	for {
		if num_matches >= 2 {
			printoutSearchFinished()
			return
		}
		remote_file_lock.Lock()
		for file_name, results := range remote_files {

			if len(results) == 0 { continue }

			hex_hash := hex.EncodeToString(results[0].MetafileHash)
			local_chunks_lock.Lock()
			metadata, ok := local_chunks[hex_hash]
			local_chunks_lock.Unlock()
			if !ok { continue }

			// Determine if all chunks of file are available at some node
			num_chunks := len(metadata)/32
			chunk_map := make(map[uint64]bool)

			for _,result := range results {
				for _,chunk_id := range result.ChunkMap {
					chunk_map[chunk_id] = true
				}
			}

			for i := 1; i <= num_chunks; i++ {
				_,ok = chunk_map[uint64(i)]
				if !ok { continue }
			}

			matches_lock.Lock()
			_,ok = matches_map[file_name]
			if !ok {
				num_matches++
				matches_map[file_name] = hex_hash
				match_order = append(match_order, file_name + ":" + hex_hash)
			}
			matches_lock.Unlock()
		}
		remote_file_lock.Unlock()

		select {
		case <-resend.C:
			budget *= 2
			global_budget = budget
			if budget > MAX_BUDGET {
				printoutSearchFinished()
				return
			}
			fmt.Println("Initiating search with budget", budget)
			go initiateFileSearch(keywords, budget)
		default:
		}
	}
}

func servefileSearchResults(w http.ResponseWriter, r *http.Request) {
	matches_lock.Lock()
	matches_json,_ := json.Marshal(match_order)
	matches_lock.Unlock()

	w.Write(matches_json)
}

func fileUpload(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	file_name := r.PostForm.Get("file_name")
	scanInFile(file_name)
}

func fileDownload(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dest := r.PostForm.Get("dest")
	file_name := r.PostForm.Get("file_name")
	hex_hash := r.PostForm.Get("hex_hash")
	if dest != "" {
		downloadFile(dest, file_name, hex_hash)
	} else {
		fmt.Println("Downloading a remote file")
		downloadRemoteFile(file_name, hex_hash)
	}
}

func fileListLocal(w http.ResponseWriter, r *http.Request) {
	local_files_lock.Lock()
	local_files_json,_ := json.Marshal(local_files)
	local_files_lock.Unlock()

	w.Write(local_files_json)
}

func changeId(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	status_vector_lock.Lock()
	name = vars["id"]
	local_id = 0
	status_vector_lock.Unlock()
	w.WriteHeader(http.StatusOK)
}

func serveId(w http.ResponseWriter, r *http.Request) {
	id_json,_ := json.Marshal(name)
	w.Write(id_json)
}

func newNode(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	peer_addr_string := r.PostForm.Get("address")
	peer_addr, err := net.ResolveUDPAddr("udp4", peer_addr_string)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}

	peers_map_lock.Lock()
	_, ok := peers_map[peer_addr.String()]
	if !ok {
		// Add to peers if new peer
		peers_map[peer_addr.String()] = true
		peers = append(peers, peer_addr.String())
	}
	peers_map_lock.Unlock()

	peers_json,_ := json.Marshal(peers)
	w.Write(peers_json)
}

func serveNodes(w http.ResponseWriter, r *http.Request) {
	json_body, _ :=json.Marshal(peers)
	w.Write(json_body)
}

func servePeers(w http.ResponseWriter, r *http.Request) {
	json_body, _ := json.Marshal(next_hop)
	w.Write(json_body)
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	text := r.PostForm.Get("text")

	message := &protocol.GossipPacket{Rumor: &protocol.RumorMessage{Origin:"client", PeerMessage: protocol.PeerMessage{ID: 0, Text:text}}}
	go sendClientMessage(message)

	w.WriteHeader(http.StatusOK)
}

func serveMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	num_messages,_ := strconv.Atoi(vars["num_messages"])

	if len(global_messages) == 0 || num_messages >= len(global_messages){
		w.Write(nil)
		return
	}

	json_body,_ := json.Marshal(global_messages[num_messages:])
	w.Write(json_body)
}

func sendPrivateMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peer := vars["peer"]

	r.ParseForm()
	text := r.PostForm.Get("text")

	go sendPrivate(peer, text)

	w.WriteHeader(http.StatusOK)
}

func servePrivateMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peer_name := vars["peer"]
	num_messages,_ := strconv.Atoi(vars["num_messages"])

	pms,ok := private_messages[peer_name]
	if !ok || len(pms) == 0 || num_messages >= len(pms){
		w.Write(nil)
		return
	}

	json_body,_ := json.Marshal(pms[num_messages:])
	w.Write(json_body)
}

func scanInFile(file_name string) {
	file, err := os.Open(DEFAULT_FILE_PATH + file_name) // For read access.
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	metafile, err := os.Create(DEFAULT_FILE_PATH + file_name + ".meta")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer metafile.Close()

	var chunks [][]byte = chunkFile(file)
	hashes := make([][]byte,0)
	for _,chunk := range chunks {
		hash := hashBytes(chunk)
		local_chunks_lock.Lock()
		local_chunks[hex.EncodeToString(hash)] = chunk
		local_chunks_lock.Unlock()
		hashes = append(hashes, hash)
	}

	metafile_contents := bytes.Join(hashes,nil)

	metafile.WriteAt(metafile_contents, 0)

	metahash := hashBytes(metafile_contents)
	local_chunks_lock.Lock()
	local_chunks[hex.EncodeToString(metahash)] = metafile_contents
	local_chunks_lock.Unlock()

	local_files_lock.Lock()
	local_files[file_name] = hex.EncodeToString(metahash)
	local_files_lock.Unlock()
}

func chunkFile(file *os.File) ([][]byte){
	chunks := make([][]byte, 0)
	file.Seek(0, 0)
	for {
		b := make([]byte, 8000)
		n, err := file.Read(b)
		if n == 0 || (err != nil && err != io.EOF) {
			fmt.Println(err)
			break
		}

		chunks = append(chunks, b[:n])

		if err == io.EOF {
			break
		}
	}

	return chunks
}

// Takes a file and returns which chunks (indices) are stored locally
// WARNING: doesn't lock on local_chunks, so only call from a locked context
func listLocalChunks(file_name string, hex_meta_hash string) []uint64{
	metafile,ok := local_chunks[hex_meta_hash]
	// If we don't have the metafile (shouldn't happen), just go to the next match
	if !ok { return nil }

	chunk_nums := make([]uint64, 0)
	for i := 0; i < len(metafile); i+=32 {
		chunk_hash := metafile[i:i+32]
		_,ok := local_chunks[hex.EncodeToString(chunk_hash)]
		if ok {
			chunk_nums = append(chunk_nums, uint64(i/32 + 1))
		}
	}
	return chunk_nums
}

func hashBytes(bytes []byte) []byte {
	h := sha256.New()
	h.Write(bytes)
	return h.Sum(nil)
}

func initiateFileSearch(keywords []string, budget uint64) {
	continueFileSearch(nil, name, keywords, budget)
}

func continueFileSearch(last_peer *net.UDPAddr, origin string, keywords []string, budget uint64) {
	num_peers := len(peers)

	if budget <= 0 { return }

	if budget < uint64(num_peers) {
		for i:=0; uint64(i) < budget; i++ {
			peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
			peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)

			if peer_addr == last_peer {
				i--
				continue
			}

			sendSearchRequest(peer_addr, origin,1, keywords)
		}
	} else {
		split_budget := splitBudget(budget)
		if split_budget == nil { return }
		for i:=0; i < len(split_budget); i++ {
			peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
			peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)

			if peer_addr == last_peer {
				i--
				continue
			}

			sendSearchRequest(peer_addr, origin, split_budget[i], keywords)
		}
	}
}

func splitBudget(budget uint64) []uint64 {
	split_len := len(peers)
	if split_len == 0 { return nil}
	budget_per_peer := make([]uint64, split_len)
	for i := range budget_per_peer {
		budget_per_peer[i] = 0
	}
	for i:=1; uint64(i)<=budget; i++ {
		index := i
		if index >= split_len {
			index = 0
		}
		budget_per_peer[index] += 1
	}

	return budget_per_peer
}

func downloadRemoteFile(file_name string, hex_hash string) {
	remote_file_lock.Lock()
	results,ok := remote_files[file_name]
	remote_file_lock.Unlock()
	// If no matches in remote files, we don't know where to get the chunks
	if !ok { return }

	// We should already have the metafile if the file is in remote_files
	local_chunks_lock.Lock()
	metadata := local_chunks[hex_hash]
	local_chunks_lock.Unlock()

	// Get chunks from each peer
	for _,result := range results {
		go downloadChunks(result.Origin, file_name, result.ChunkMap, metadata)
	}

	// Check if we have all the chunks
	check_timer := time.NewTicker(1*time.Second)
	for {
		select {
		case <-check_timer.C:
			i := 0
			for i < len(metadata)/32 {
				chunk_hash := metadata[32*i:32*i+32]
				local_chunks_lock.Lock()
				_,ok = local_chunks[hex.EncodeToString(chunk_hash)]
				local_chunks_lock.Unlock()
				if !ok {
					break
				}
				if i == len(metadata)/32 - 1 {
					printoutReconstructed(file_name)
					writeDownloadToDisk(file_name, metadata)
					return
				}
			}
		}
	}
}

func downloadChunks(dest string, file_name string, chunks []uint64, metadata []byte) {
	chunk_hashes := make([][]byte,0)
	if chunks == nil {
		i := 0
		for i < len(metadata)/32 {
			chunk_hashes = append(chunk_hashes, metadata[32*i:32*i+32])
			i++
		}
	} else {
		for i := 0; i < len(chunks); i++ {
			chunk_id := int(chunks[i])
			chunk_hash := metadata[32 * (chunk_id - 1): 32 * (chunk_id - 1) + 32]
			chunk_hashes = append(chunk_hashes, chunk_hash)
		}
	}

	var resend *time.Ticker = time.NewTicker(time.Duration(RESEND_TIMEOUT) * time.Second)

	// Get chunks sequentially
	// Retry every 5 seconds if no response
	current_chunk := chunk_hashes[0]
	current_chunk_index := 0
	local_chunks_lock.Lock()
	_,ok := local_chunks[hex.EncodeToString([]byte(current_chunk))]
	local_chunks_lock.Unlock()
	if !ok {
		printoutDownloadingChunk(file_name, 1, dest)
		sendDataRequest(dest, file_name, hex.EncodeToString([]byte(current_chunk)))
		resend = time.NewTicker(time.Duration(RESEND_TIMEOUT) * time.Second)
	}

	for {
		local_chunks_lock.Lock()
		_,ok := local_chunks[hex.EncodeToString([]byte(current_chunk))]
		local_chunks_lock.Unlock()
		if ok {
			if current_chunk_index == len(chunk_hashes) - 1 {
				return
			}

			for index,chunk_hash := range chunk_hashes {
				local_chunks_lock.Lock()
				_,ok := local_chunks[hex.EncodeToString([]byte(chunk_hash))]
				local_chunks_lock.Unlock()
				if !ok {
					printoutDownloadingChunk(file_name, index + 1, dest)
					current_chunk = []byte(chunk_hash)
					current_chunk_index = index
					// Send request for next chunk and reset ticker
					sendDataRequest(dest, file_name, hex.EncodeToString([]byte(current_chunk)))
					resend = time.NewTicker(time.Duration(RESEND_TIMEOUT) * time.Second)
					break
				}
			}

		}

		select {
		case <-resend.C:
			sendDataRequest(dest, file_name, hex.EncodeToString([]byte(current_chunk)))
		default:
		}
	}
}

func downloadFile(dest string, file_name string, hex_hash string) {
	local_files_lock.Lock()
	local_files[file_name] = hex_hash
	local_files_lock.Unlock()


	local_chunks_lock.Lock()
	var metadata []byte
	metadata,ok := local_chunks[hex_hash]
	local_chunks_lock.Unlock()

	if !ok {
		metadata = downloadMetafile(dest, file_name, hex_hash)
		local_chunks[hex_hash] = metadata
	}


	downloadChunks(dest, file_name, nil, metadata)

	printoutReconstructed(file_name)

	writeDownloadToDisk(file_name, metadata)
}

func writeDownloadToDisk(file_name string, metadata []byte) {
	file, err := os.Create(DEFAULT_FILE_PATH + file_name) // For read access.
	if err != nil {
		fmt.Println(err)
		return
	}

	chunk_hashes := make([][]byte,0)
	i := 0
	for i < len(metadata)/32 {
		chunk_hashes = append(chunk_hashes, metadata[32*i:32*i+32])
		i++
	}

	local_chunks_lock.Lock()
	for index,chunk_hash := range chunk_hashes {
		data,_ := local_chunks[hex.EncodeToString([]byte(chunk_hash))]
		file.WriteAt(data, int64(index * 8000))
	}
	local_chunks_lock.Unlock()
	file.Close()
}

func downloadMetafile(dest string, file_name string, hex_hash string) []byte{
	printoutDownloadingMetafile(file_name, dest)
	sendDataRequest(dest, file_name, hex_hash)

	var metadata []byte

	resend := time.NewTicker(time.Duration(RESEND_TIMEOUT) * time.Second)

	// Resend until we get the metadata
	for {
		local_chunks_lock.Lock()
		var ok bool
		metadata,ok = local_chunks[hex_hash]
		local_chunks_lock.Unlock()

		if ok {break}

		select {
		case <-resend.C:
			sendDataRequest(dest, file_name, hex_hash)
		default:
		}
	}

	return metadata
}

func readClient() {
	for {
		_, buf, err := readFromUDPConn(client_conn)
		if err != nil {
			fmt.Println(err)
		}

		message, err := protocol.Decode(*buf)
		if err != nil {
			fmt.Println("Error decoding", err)
			continue
		}
		if message.Rumor != nil {
			sendClientMessage(message)
			global_messages = append(global_messages, message)
		} else if message.Private != nil {
			sendClientPrivateMessage(message)
		} else if message.DataRequest != nil {
			downloadFile(message.DataRequest.Destination,
						 message.DataRequest.FileName,
						 hex.EncodeToString(message.DataRequest.HashValue))
		} else if message.SearchRequest != nil {
			searchForFile(message.SearchRequest.Keywords, message.SearchRequest.Budget)
		}
	}
}

func sendClientPrivateMessage(message *protocol.GossipPacket) {
	dest := message.Private.Dest
	text := message.Private.Text
	go sendPrivate(dest, text)
}

func sendClientMessage(message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	// Update the message fields
	message.Rumor.Origin = name
	local_id++
	message.Rumor.PeerMessage.ID = local_id

	printoutMessageReceived(nil, message, true)

	global_messages = append(global_messages, message)

	// Add origin to status vector if new
	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	// Update the status vector
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)

	// Abort if there are no peers
	if len(peers) == 0 {
		return
	}

	// Create a permutation of known peers
	remaining_addrs := make([]string, len(peers))
	copy(remaining_addrs, peers)
	for i := range remaining_addrs {
		j := rand.Intn(i + 1)
		remaining_addrs[i], remaining_addrs[j] = remaining_addrs[j], remaining_addrs[i]
	}

	// Resolve the peer address and start gossiping
	new_peer_addr_str := remaining_addrs[0]
	new_peer_addr, err := net.ResolveUDPAddr("udp4", new_peer_addr_str)
	if err != nil {
		fmt.Println(err)
	}
	go startGossiping(new_peer_addr, message)
}

func readGossip() {
	for {
		peer_addr, buf, err := readFromUDPConn(gossip_conn)
		if err != nil {
			fmt.Println(err)
		}

		go processMessage(peer_addr, buf)
	}
}

func periodicEvents() {
	anti_entropy := time.NewTicker(time.Second)
	route_rumor := time.NewTicker(/*time.Duration(rtimer)*/ 1 * time.Second)

	// We want to send a route rumor upon startup, then based on timer
	if len(peers) > 0 {
		for _,peer_addr_str := range peers {
			new_peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
			printoutMongeringRoute(new_peer_addr)
			// Send the message
			sendRouteRumor(new_peer_addr)
		}
	}

	for {
		select {
			case <-anti_entropy.C:
				// pick a random peer
				if len(peers) > 0 {
					peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
					peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
					// send a status message
					sendStatus(peer_addr)
				}
				break
			case <-route_rumor.C:
				// pick a random peer
				if len(peers) > 0 {
					peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
					peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
					// send a status message
					sendRouteRumor(peer_addr)
				}
				break
		}
	}
}

func processMessage(peer_addr *net.UDPAddr, buf *[]byte) {
	message, err := protocol.Decode(*buf)
	if err != nil {
		fmt.Println("Error decoding", err)
		fmt.Println(len(*buf))
		return
	}

	num_msg_types := 0
	if message.Rumor!=nil {num_msg_types++}
	if message.Status!=nil {num_msg_types++}
	if message.Private!=nil {num_msg_types++}
	if message.DataRequest!=nil {num_msg_types++}
	if message.DataReply!=nil {num_msg_types++}
	if message.SearchRequest!=nil {num_msg_types++}
	if message.SearchReply!=nil {num_msg_types++}
	if num_msg_types != 1{
		fmt.Println("Malformed Message: more than one component")
		return
	}

	// Add to peers if new peer
	/*peers_map_lock.Lock()
	_, ok := peers_map[peer_addr.String()]
	if !ok {
		peers_map[peer_addr.String()] = true
		peers = append(peers, peer_addr.String())
	}
	peers_map_lock.Unlock()*/

	printoutMessageReceived(peer_addr, message, false)

	if message.Rumor != nil {
		processRumor(peer_addr, message)
	} else if message.Status != nil {
		processStatus(peer_addr, message)
	} else if message.SearchRequest != nil {
		processSearchRequest(peer_addr, message)
	} else {
		processP2P(message)
	}
}

func processRumor(peer_addr *net.UDPAddr,message *protocol.GossipPacket) {
	// Send a status as an ACK
	defer func() {sendStatus(peer_addr)}()

	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	/*peers_map_lock.Lock()
	// Add to last ip and port if new
	if message.Rumor.LastIP != nil && message.Rumor.LastPort != nil {
		last_addr := message.Rumor.LastIP.String() + ":" + strconv.Itoa(*message.Rumor.LastPort)
		_, ok := peers_map[last_addr]
		if !ok && last_addr != gossip_addr.String(){
			peers_map[last_addr] = true
			peers = append(peers, last_addr)
		}
	}
	peers_map_lock.Unlock()*/

	// Add origin to status vector if new
	messages,ok := status_vector[message.Rumor.Origin]
	if !ok {
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
		messages = status_vector[message.Rumor.Origin]
	}

	// Update the next hop routing table
	next_hop_lock.Lock()
	hop, ok := next_hop[message.Rumor.Origin]
	direct := message.Rumor.LastIP == nil
	if !ok {
		next_hop[message.Rumor.Origin] = Hop{
			Id: message.Rumor.ID,
			Direct: direct,
			Next: peer_addr.String(),
		}
		if direct {
			printoutDirectRoute(message.Rumor.Origin, peer_addr.String())
		} else {
			printoutDSDV(message.Rumor.Origin, peer_addr.String())
		}
	} else {
		if message.Rumor.ID > hop.Id ||
			(message.Rumor.ID == hop.Id && direct) {
			hop.Next = peer_addr.String()
			hop.Id = message.Rumor.ID
			hop.Direct = direct
			next_hop[message.Rumor.Origin] = hop
			if direct {
				printoutDirectRoute(message.Rumor.Origin, peer_addr.String())
			} else {
				printoutDSDV(message.Rumor.Origin, peer_addr.String())
			}
		}
	}
	next_hop_lock.Unlock()

	// Check if this is the next message we want from the origin
	if message.Rumor.PeerMessage.ID != uint32(len(messages)) { return }

	// Store the rumor
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)
	// Only make messages with non-empty text client accessible
	if message.Rumor.Text != "" {
		global_messages = append(global_messages, message)
	}

	// Drop if no_forward and a non-empty text
	if no_forward && message.Rumor.Text!="" { return }

	// Pick a random peer (other than sender) and start gossiping with them
	if len(peers) <= 1 { return }

	// Update last ip and port
	message.Rumor.LastIP = &peer_addr.IP
	message.Rumor.LastPort = &peer_addr.Port

	// Send to all peers immediately if it's a route rumor
	if message.Rumor.Text == "" {
		for _,peer_addr_str := range peers {
			if peer_addr_str == peer_addr.String() {continue}
			new_peer_addr, _ := net.ResolveUDPAddr("udp4", peer_addr_str)
			printoutMongeringRoute(new_peer_addr)
			// Send the message
			message_bytes, _ := protocol.Encode(message)
			gossip_conn.WriteToUDP(message_bytes, new_peer_addr)
		}
	} else {
		new_peer_addr_str := peers[int(rand.Float32()*float32(len(peers)))]
		for {
			if new_peer_addr_str != peer_addr.String() {
				break
			}
			new_peer_addr_str = peers[int(rand.Float32()*float32(len(peers)))]
		}
		new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)

		go startGossiping(new_peer_addr, message)
	}
}

func processStatus(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	// Compare the status vectors
	for _,peer_status := range message.Status.Want {
		messages, ok := status_vector[peer_status.Identifier]
		if !ok {
			// Add origin to status vector if new
			status_vector[peer_status.Identifier] = make([]*protocol.GossipPacket, 1)
			messages = status_vector[peer_status.Identifier]
		}

		// If we're ahead, get the peer up-to-date
		// Get message from peer[identifier] with id NextID
		// If we're behind, ask the peer to get us up-to-date
		if uint32(len(messages)) > peer_status.NextID {
			// If we're allowed to forward, send the message to the peer
			if no_forward {continue}

			message_to_send := messages[peer_status.NextID]
			message_bytes, _ := protocol.Encode(message_to_send)
			gossip_conn.WriteToUDP(message_bytes, peer_addr)
			return
		} else if uint32(len(messages)) < peer_status.NextID {
			go sendStatus(peer_addr)
			return
		}
	}
	printoutInSync(peer_addr)
}

func processSearchRequest(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	// If it's our request, drop the packet
	if message.SearchRequest.Origin == name || message.SearchRequest.Budget <= 0{
		return
	}

	// Drop packet if we received a duplicate less than .5 seconds ago
	kwrds := strings.Join(message.SearchRequest.Keywords, ",")
	key := strings.Join([]string{message.SearchRequest.Origin, kwrds}, ":")
	_,found := request_cache.Get(key)
	if found {
		return
	} else {
		request_cache.Set(key, true, cache.DefaultExpiration)
	}

	// Forward the request to additional peers
	// Do this first so that local IO isn't a bottleneck for propagation
	// Decrease budget by one
	continueFileSearch(peer_addr,
					   message.SearchRequest.Origin,
					   message.SearchRequest.Keywords,
				message.SearchRequest.Budget - 1)

	// Search our store of filenames for keyword matches
	matches := make(map[string]string)
	local_files_lock.Lock()
	for file_name, hex_meta_hash := range local_files {
		for _,keyword := range message.SearchRequest.Keywords {
			if strings.Contains(file_name, keyword) {
				matches[file_name] = hex_meta_hash
			}
		}
	}
	local_files_lock.Unlock()

	// Check if we have chunks for any matching files we find
	results := make([]*protocol.SearchResult, 0)
	for file_name, hex_meta_hash := range matches {
		local_chunks_lock.Lock()
		chunk_ids := listLocalChunks(file_name, hex_meta_hash)
		local_chunks_lock.Unlock()
		if chunk_ids != nil && len(chunk_ids) > 0 {
			metafile_hash,_ := hex.DecodeString(hex_meta_hash)
			result := &protocol.SearchResult{
				FileName: file_name,
				MetafileHash: metafile_hash,
				ChunkMap: chunk_ids,
			}
			results = append(results, result)
		}
	}

	// Send a SearchReply back to the original querent
	sendSearchReply(message.SearchRequest.Origin, results)
}

func processP2P(message *protocol.GossipPacket) {
	var dest string

	if message.Private != nil {
		dest = message.Private.Dest
		if dest == name {
			processPrivate(message)
			return
		}

		if message.Private.HopLimit <= 1 { return }

		// Decrement the hop limit to combat routing loops
		message.Private.HopLimit -= 1
	} else if message.DataRequest != nil {
		dest = message.DataRequest.Destination
		if dest == name {
			processDataRequest(message)
			return
		}

		if message.DataRequest.HopLimit <= 1 { return }

		// Decrement the hop limit to combat routing loops
		message.DataRequest.HopLimit -= 1
	} else if message.DataReply != nil {
		dest = message.DataReply.Destination
		if dest == name {
			processDataReply(message)
			return
		}

		if message.DataReply.HopLimit <= 1 { return }

		// Decrement the hop limit to combat routing loops
		message.DataReply.HopLimit -= 1
	} else {
		dest = message.SearchReply.Destination
		if dest == name {
			processSearchReply(message)
			return
		}

		if message.SearchReply.HopLimit <= 1 { return }

		// Decrement the hop limit to combat routing loops
		message.SearchReply.HopLimit -= 1
	}

	// Drop the message if we're not allowed to forward and it's not for us
	if no_forward {
		fmt.Println("Not forwarding private message")
		return
	}

	sendP2P(message, dest)
}

func processPrivate(message *protocol.GossipPacket) {
	private_messages_lock.Lock()
	_,ok := private_messages[message.Private.Origin]
	if !ok {
		// Add origin to private_messages keys if new
		private_messages[message.Private.Origin] = make([]*protocol.GossipPacket, 0)
	}
	private_messages[message.Private.Origin] = append(private_messages[message.Private.Origin], message)
	private_messages_lock.Unlock()
	return
}

func processDataRequest(message *protocol.GossipPacket) {
	data, ok := local_chunks[hex.EncodeToString(message.DataRequest.HashValue)]
	if !ok {
		fmt.Println("Don't have the requested data")
		return
	}

	sendDataReply(message.DataRequest, data)
}

func processDataReply(message *protocol.GossipPacket) {
	if bytes.Compare(message.DataReply.HashValue, hashBytes(message.DataReply.Data)) != 0{
		// Hashes don't match, so drop packet
		fmt.Println("Hash doen't match data, dropping packet")
		return
	}

	local_chunks_lock.Lock()
	local_chunks[hex.EncodeToString(message.DataReply.HashValue)] = message.DataReply.Data
	local_chunks_lock.Unlock()
}

func processSearchReply(message *protocol.GossipPacket) {
	// Add each result to the store of results for its corresponding file
	for _,result := range message.SearchReply.Results {
		remote_file_lock.Lock()
		existing_results, ok := remote_files[result.FileName]
		remote_file_lock.Unlock()

		// If this is a "new" file, instantiate result store
		if !ok {
			printoutFoundMatch(result, message.SearchReply)
			remote_file_lock.Lock()
			remote_files[result.FileName] = make([]*RemoteChunks,0)
			existing_results = remote_files[result.FileName]
			remote_file_lock.Unlock()

			// If this is a truly new file, download its metafile
			local_chunks_lock.Lock()
			_,ok = local_chunks[hex.EncodeToString(result.MetafileHash)]
			local_chunks_lock.Unlock()
			if !ok {
				metadata := downloadMetafile(message.SearchReply.Origin, result.FileName, hex.EncodeToString(result.MetafileHash))
				local_chunks_lock.Lock()
				local_chunks[hex.EncodeToString(result.MetafileHash)] = metadata
				local_chunks_lock.Unlock()
			}
		}

		rc := &RemoteChunks{Origin:message.SearchReply.Origin, ChunkMap:result.ChunkMap, MetafileHash:result.MetafileHash}
		existing_results = append(existing_results, rc)
		remote_file_lock.Lock()
		remote_files[result.FileName] = existing_results
		remote_file_lock.Unlock()

	}
}

func sendSearchRequest(addr *net.UDPAddr, origin string, budget uint64, keywords []string) {
	message := &protocol.GossipPacket{
		SearchRequest: &protocol.SearchRequest{
			Origin:   origin,
			Budget:   budget,
			Keywords: keywords,
		},
	}

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendSearchReply(dest string, results []*protocol.SearchResult) {
	message := &protocol.GossipPacket{
		SearchReply: &protocol.SearchReply{
			Origin: name,
			Destination: dest,
			HopLimit: DEFAULT_HOP_LIMIT,
			Results: results,
		},
	}

	sendP2P(message, dest)
}

func sendStatus(addr *net.UDPAddr) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	peer_statuses := make([]protocol.PeerStatus, 0)
	for id,messages := range status_vector {
		next_id := uint32(len(messages))
		peer_statuses = append(peer_statuses, protocol.PeerStatus{Identifier:id, NextID:next_id})
	}

	message := &protocol.GossipPacket{
		Status: &protocol.StatusPacket{
			Want:peer_statuses,
		},
	}

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendRouteRumor(addr *net.UDPAddr) {
	status_vector_lock.Lock()
	defer status_vector_lock.Unlock()

	message := &protocol.GossipPacket{
		Rumor: &protocol.RumorMessage{
			Origin:name,
			PeerMessage: protocol.PeerMessage{
				ID: 0,
				Text:"",
			},
		},
	}

	local_id++
	message.Rumor.PeerMessage.ID = local_id

	_,ok := status_vector[message.Rumor.Origin]
	if !ok {
		// Add origin to status vector if new
		status_vector[message.Rumor.Origin] = make([]*protocol.GossipPacket, 1)
	}
	status_vector[message.Rumor.Origin] = append(status_vector[message.Rumor.Origin], message)

	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, addr)
}

func sendPrivate(dest string, text string) {
	if dest=="" || text=="" {return}

	private_messages_lock.Lock()
	defer private_messages_lock.Unlock()

	// Create message, set hop limit to default
	message := &protocol.GossipPacket{
		Private: &protocol.PrivateMessage{
			Origin:name,
			Dest:dest,
			HopLimit: DEFAULT_HOP_LIMIT,
			PeerMessage: protocol.PeerMessage{
				ID:0,
				Text:text,
			},
		},
	}

	// Initialize conversation if first message for destination
	_,ok := private_messages[dest]
	if !ok {
		// Add origin to private_messages keys if new
		private_messages[dest] = make([]*protocol.GossipPacket, 0)
	}
	private_messages[dest] = append(private_messages[dest], message)
	sendP2P(message, dest)
}

func sendDataReply(request *protocol.DataRequest, data []byte) {
	reply := &protocol.GossipPacket{
		DataReply: &protocol.DataReply{
			Origin:      name,
			Destination: request.Origin,
			HopLimit:    DEFAULT_HOP_LIMIT,
			FileName:    request.FileName,
			HashValue:   request.HashValue,
			Data:        data,
		},
	}
	sendP2P(reply, request.Origin)
}

func sendDataRequest(dest string, file_name string, hex_hash string) {
	hash,_ := hex.DecodeString(hex_hash)
	request := &protocol.GossipPacket{
		DataRequest: &protocol.DataRequest{
			Origin:      name,
			Destination: dest,
			HopLimit:    DEFAULT_HOP_LIMIT,
			FileName:    file_name,
			HashValue:   hash,
		},
	}
	sendP2P(request, dest)
}

func sendP2P(message *protocol.GossipPacket, dest string) {
	next_hop_lock.Lock()
	next,ok := next_hop[dest]
	next_hop_lock.Unlock()
	// Abort if no known route
	if !ok {return}

	// Resolve the next hop address and send the message
	next_addr, _ := net.ResolveUDPAddr("udp4", next.Next)
	message_bytes,_ := protocol.Encode(message)

	gossip_conn.WriteToUDP(message_bytes, next_addr)
}

func startGossiping(peer_addr *net.UDPAddr, message *protocol.GossipPacket) {
	printoutMongeringText(peer_addr)

	// Send the message
	message_bytes, _ := protocol.Encode(message)
	gossip_conn.WriteToUDP(message_bytes, peer_addr)

	// Flip a coin and check if there are new peers to gossip with
	if rand.Float32() >= 0.5 && len(peers) > 1{
		// Pick a new peer to gossip with
		new_peer_addr_str := peers[int(rand.Float32() * float32(len(peers)))]
		for {
			if new_peer_addr_str == peer_addr.String() {
				new_peer_addr_str = peers[int(rand.Float32() * float32(len(peers)))]
				continue
			}
			break
		}
		new_peer_addr, _ := net.ResolveUDPAddr("udp4", new_peer_addr_str)

		printoutFlipPassed(new_peer_addr)

		// Start gossiping with the new peer
		go startGossiping(new_peer_addr, message)
	}
}

func readFromUDPConn(conn *net.UDPConn) (*net.UDPAddr, *[]byte, error) {
	buf := make([]byte, 10000)
	num_bytes, addr, err := conn.ReadFromUDP(buf[0:])

	if err != nil {
		return addr, nil, err
	}

	buf = buf[0:num_bytes]
	return addr, &buf, nil
}

// Assumes the message is well-formed
func printoutMessageReceived(peer_addr *net.UDPAddr, message *protocol.GossipPacket, client bool) {
	//if true {return}

	if client {
		fmt.Println("CLIENT", message.Rumor.PeerMessage.Text, message.Rumor.Origin)
	} else {
		if message.Rumor != nil {
			fmt.Println("RUMOR origin", message.Rumor.Origin, "from", peer_addr.String(), "ID", message.Rumor.PeerMessage.ID, "contents", message.Rumor.PeerMessage.Text)
		} else if message.Status != nil{
			fmt.Print("STATUS from ", peer_addr.String())
			for _,peer_status := range message.Status.Want {
				fmt.Print(" origin ", peer_status.Identifier, " nextID ", peer_status.NextID)
			}
			fmt.Println()
		} else if message.Private != nil{
			fmt.Printf("PRIVATE: %s:%d:%s\n", message.Private.Origin, message.Private.HopLimit-1, message.Private.Text)
		}
		fmt.Println(strings.Join(peers, ","))
	}

}

func printoutDSDV(origin string, addr string) {
	//if true {return}
	fmt.Printf("DSDV %s: %s\n", origin, addr)
}

func printoutDirectRoute(origin string, addr string) {
	//if true {return}
	fmt.Printf("DIRECT-ROUTE FOR %s: %s\n", origin, addr)
}

func printoutMongeringRoute(peer_addr *net.UDPAddr) {
	//if true {return}
	fmt.Println("MONGERING ROUTE to", peer_addr.String())
}

func printoutMongeringText(peer_addr *net.UDPAddr) {
	//if true {return}
	fmt.Println("MONGERING TEXT to", peer_addr.String())
}

func printoutFlipPassed(peer_addr *net.UDPAddr) {
	//if true {return}
	fmt.Println("FLIPPED COIN sending rumor to", peer_addr.String())
}

func printoutInSync(peer_addr *net.UDPAddr) {
	//if true {return}
	fmt.Println("IN SYNC WITH", peer_addr.String())
}

func printoutDownloadingMetafile(file_name string, peer string) {
	//if true {return}
	fmt.Println("DOWNLOADING metafile of", file_name, "from", peer)
}

func printoutDownloadingChunk(file_name string, chunk_num int, peer string) {
	//if true {return}
	fmt.Println("DOWNLOADING", file_name, "chunk", chunk_num, "from", peer)
}

func printoutReconstructed(file_name string) {
	fmt.Println("RECONSTRUCTED file", file_name)
}

func printoutFoundMatch(result *protocol.SearchResult, reply *protocol.SearchReply) {
	budget_str := "budget="+strconv.Itoa(int(global_budget))
	meta_str := "metafile="+hex.EncodeToString(result.MetafileHash)
	chunks := "chunks="
	for i,chunk := range result.ChunkMap {
		if i > 0 {
			chunks += ","
		}
		chunks += strconv.Itoa(int(chunk))
	}
	fmt.Println("FOUND match", result.FileName, "at", reply.Origin, budget_str, meta_str, chunks)
}

func printoutSearchFinished() {
	fmt.Println("SEARCH FINISHED")
}