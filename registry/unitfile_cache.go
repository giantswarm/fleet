package registry

import (
	"sync"
	"time"

	"github.com/coreos/fleet/unit"
)

type unitFileCacheEntry struct {
	unitFile   *unit.UnitFile
	lastAccess time.Time
}

type unitFileCacheT struct {
	unitFiles map[unit.Hash]*unitFileCacheEntry
	mu        *sync.Mutex
}

var unitFileCache unitFileCacheT

func init() {
	unitFileCache = unitFileCacheT{map[unit.Hash]*unitFileCacheEntry{}, new(sync.Mutex)}

	// cleanup old entries every 30 minutes
	go func() {
		for _ = range time.Tick(30 * time.Minute) {
			unitFileCache.cleanupOlderThan(30 * time.Minute)
		}
	}()
}

func (uf *unitFileCacheT) cleanupOlderThan(maxAge time.Duration) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	lastAccessLimit := time.Now().Add(-maxAge)

	for k, v := range uf.unitFiles {
		if v.lastAccess.Before(lastAccessLimit) {
			delete(uf.unitFiles, k)
		}
	}
}

func (uf *unitFileCacheT) get(hash unit.Hash) *unit.UnitFile {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	if cacheEntry, exists := uf.unitFiles[hash]; exists {
		cacheEntry.lastAccess = time.Now()
		return cacheEntry.unitFile
	}
	return nil
}

func (uf *unitFileCacheT) cache(hash unit.Hash, unitFile *unit.UnitFile) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	uf.unitFiles[hash] = &unitFileCacheEntry{unitFile, time.Now()}
}
