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
	unitFiles map[string]*unitFileCacheEntry
	mu        *sync.Mutex
}

var unitFileCache unitFileCacheT

func init() {
	unitFileCache = unitFileCacheT{map[string]*unitFileCacheEntry{}, new(sync.Mutex)}

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

func (uf *unitFileCacheT) get(hash unit.Hash, jobName string) *unit.UnitFile {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	key := uf.createKey(hash, jobName)
	if cacheEntry, exists := uf.unitFiles[key]; exists {
		cacheEntry.lastAccess = time.Now()
		return cacheEntry.unitFile
	}
	return nil
}

func (uf *unitFileCacheT) cache(hash unit.Hash, jobName string, unitFile *unit.UnitFile) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	key := uf.createKey(hash, jobName)
	uf.unitFiles[key] = &unitFileCacheEntry{unitFile, time.Now()}
}

func (uf *unitFileCacheT) createKey(hash unit.Hash, jobName string) string {
	return hash.String() + "-" + jobName
}
