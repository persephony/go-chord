package chord

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
)

var defaultBucketName = []byte("default")

// Used by chord
//
// Store manager for vnodes on a given node
//
type BoltdbStore struct {
	mu sync.Mutex
	a  []*BoltdbDatastore

	dataDir string
}

func NewBoltdbStore(dataDir string) *BoltdbStore {
	return &BoltdbStore{
		a:       []*BoltdbDatastore{},
		dataDir: dataDir,
	}
}

func (s *BoltdbStore) New() KVStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	ikv := NewBoltdbDatastore(s.dataDir)
	s.a = append(s.a, ikv)

	return ikv
}

func (s *BoltdbStore) Get(vnodeId string) (KVStore, error) {
	for _, a := range s.a {
		if a.id == vnodeId {
			return a, nil
		}
	}
	return nil, fmt.Errorf("not found: %s", vnodeId)
}

// Close all underlying stores returning the last error.
func (s *BoltdbStore) Close() error {
	var err error
	for _, v := range s.a {
		err = mergeErrors(err, v.Close())
	}
	return err
}

//
// A single datastore for a vnode
//
type BoltdbDatastore struct {
	mu sync.Mutex

	db *bolt.DB

	dataDir string
	id      string // vnode id
}

func NewBoltdbDatastore(dataDir string) *BoltdbDatastore {
	return &BoltdbDatastore{dataDir: dataDir}
}

func (s *BoltdbDatastore) Open(id string) (err error) {
	if s.db != nil {
		return fmt.Errorf("datastore already open")
	}
	os.MkdirAll(s.dataDir, 0755)

	s.id = id
	if s.db, err = bolt.Open(filepath.Join(s.dataDir, id), 0600, nil); err == nil {
		err = s.db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(defaultBucketName)
			return err
		})
	}
	return
}

func (s *BoltdbDatastore) Snapshot() (io.ReadCloser, error) {
	b, err := s.Buckets()
	if err != nil {
		return nil, err
	}
	if len(b) < 1 {
		return nil, fmt.Errorf("no buckets found")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// flush everything to disk
	s.db.Sync()
	wr := new(bytes.Buffer)
	err = s.db.View(func(tx *bolt.Tx) error {
		_, e := tx.WriteTo(wr)
		return e
	})
	rc := &DummyReadCloser{wr}
	return rc, err
}

func (s *BoltdbDatastore) Restore(r io.ReadCloser) error {
	defer r.Close()
	// Copy to temp file
	fl, err := ioutil.TempFile("/tmp", "restore-")
	if err != nil {
		return err
	}
	if _, err = io.Copy(fl, r); err != nil {
		fl.Close()
		return err
	}
	fl.Close()

	// Open received snapshot from file.
	tdb, err := bolt.Open(fl.Name(), 0600, nil)
	if err != nil {
		return err
	}
	// list buckets from r
	bkts, err := listBuckets(tdb)
	if err != nil {
		return err
	}

	if len(bkts) < 1 {
		log.Printf("loglevel=WRN action=restore msg='Nothing to restore. No buckets found'")
		return nil
	}

	//s.mu.Lock()
	//defer s.mu.Unlock()
	// read op from r
	return tdb.View(func(ttx *bolt.Tx) error {
		// write op on local
		return s.db.Batch(func(tx *bolt.Tx) error {
			// loop buckets from r and populate local
			for _, bn := range bkts {
				// get local bucket
				bkt, e1 := tx.CreateBucketIfNotExists(bn)
				if e1 != nil {
					log.Println("ERR", e1)
					continue
				}
				// get other bucket
				obkt := ttx.Bucket(bn)
				c := obkt.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if e2 := bkt.Put(k, v); e2 != nil {
						log.Printf("ERR Failed to restore key: %s", k)
						//err = mergeErrors(e, e2)
					}
				}
			}
			return nil
		})
	})
}

func (s *BoltdbDatastore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(defaultBucketName)
		if bkt == nil {
			return fmt.Errorf("bucket not found: '%s'", defaultBucketName)
		}

		if val = bkt.Get(key); val == nil || len(val) < 1 {
			return fmt.Errorf("key not found: '%s'", key)
		}
		return nil
	})
	return val, err
}

func (s *BoltdbDatastore) Set(key []byte, v []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(defaultBucketName)
		if err == nil {
			err = bkt.Put(key, v)
		}
		return err
	})

	return err
}

func (s *BoltdbDatastore) Delete(key []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(defaultBucketName)
		if bkt == nil {
			return fmt.Errorf("bucket not found: '%s'", defaultBucketName)
		}
		return bkt.Delete(key)
	})
	return err
}

func (s *BoltdbDatastore) Buckets() ([][]byte, error) {
	return listBuckets(s.db)
}

func (s *BoltdbDatastore) Close() error {
	return s.db.Close()
}

func listBuckets(db *bolt.DB) ([][]byte, error) {
	buckets := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			//fmt.Println(string(name))
			buckets = append(buckets, name)
			return nil
		})
	})

	return buckets, err
}
