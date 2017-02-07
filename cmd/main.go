package main

import (
	"flag"
	"github.com/lysu/rtvk"
	"strings"
	"github.com/coreos/etcd/raft/raftpb"
)

func main() {

	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var storage *rtvk.Storage
	getSnapshot := func() ([]byte, error) { return storage.GetSnapshot()}

	commitC, errorC, snapshotterReady := rtvk.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs := rtvk.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	rtvk.ServeHttpKVAPI(kvs, *kvport, confChangeC, errorC);
}
