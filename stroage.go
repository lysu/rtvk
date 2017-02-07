package rtvk

import (
	"sync"
	"github.com/coreos/etcd/snap"
	"log"
	"encoding/json"
	"encoding/gob"
	"bytes"
)

type Storage struct {
	proposeC chan<- string
	mu sync.RWMutex
	strore map[string]string
	snapshotter *snap.Snapshotter
}

type entry struct {
	Key string
	Value string
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *Storage {
	s := &Storage{proposeC: proposeC, strore:make(map[string]string), snapshotter:snapshotter}
	return s;
}

func (s *Storage) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue;
		}
		var dataEntry entry
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataEntry); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.strore[dataEntry.Key] = dataEntry.Value
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *Storage) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.strore = store
	s.mu.Unlock()
	return nil
}

func (s *Storage) GetSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.strore)
}

func (s *Storage) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- string(buf.Bytes())
}

func (s *Storage) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.strore[key]
	s.mu.RUnlock()
	return v, ok
}
