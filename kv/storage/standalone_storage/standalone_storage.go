package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			if err := engine_util.PutCF(s.db, put.Cf, put.Key, put.Value); err != nil {
				return err
			}
		case storage.Delete:
			delete := modify.Data.(storage.Delete)
			if err := engine_util.DeleteCF(s.db, delete.Cf, delete.Key); err != nil {
				return err
			}
		}
	}

	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

type StandAloneIterator struct {
	iter *badger.Iterator
}

func (it *StandAloneIterator) Item() engine_util.DBItem {
	item := it.iter.Item()
	return &StandAloneItem{
		item: item,
	}
}

func (it *StandAloneIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *StandAloneIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(prefix)
}

func (it *StandAloneIterator) Close() {
	it.iter.Close()
}

func (it *StandAloneIterator) Next() {
	it.iter.Next()
}

func (it *StandAloneIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *StandAloneIterator) Rewind() {
	it.iter.Rewind()
}

type StandAloneItem struct {
	item *badger.Item
}

func (item *StandAloneItem) Key() []byte {
	return item.item.KeyCopy(nil)
}

func (item *StandAloneItem) KeyCopy(dst []byte) []byte {
	return item.item.KeyCopy(dst)
}

func (item *StandAloneItem) Value() ([]byte, error) {
	return item.item.ValueCopy(nil)
}

func (item *StandAloneItem) ValueSize() int {
	return int(item.item.ValueSize())
}

func (item *StandAloneItem) ValueCopy(dst []byte) ([]byte, error) {
	return item.item.ValueCopy(dst)
}
