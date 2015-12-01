package job

import (
	"sync"
	"time"
)

type requirementsCacheEntry struct {
	requirements map[string][]string
	lastAccess   time.Time
}

type jobRequirementsCacheT struct {
	requirements map[jobRequirementsKey]*requirementsCacheEntry
	mu           *sync.Mutex
}

func init() {
	jobRequirementsCache = jobRequirementsCacheT{
		requirements: map[jobRequirementsKey]*requirementsCacheEntry{},
		mu:           new(sync.Mutex),
	}
}

var jobRequirementsCache jobRequirementsCacheT

type jobRequirementsKey struct {
	Name string
	Hash string
}

func (c *jobRequirementsCacheT) get(jobname, hash string) map[string][]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if reqEntry, exists := c.requirements[jobRequirementsKey{jobname, hash}]; exists {
		reqEntry.lastAccess = time.Now()
		return reqEntry.requirements
	}

	return nil
}

func (c *jobRequirementsCacheT) cache(jobname, hash string, requirements map[string][]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirements[jobRequirementsKey{jobname, hash}] = &requirementsCacheEntry{requirements, time.Now()}
}

func (c *jobRequirementsCacheT) cleanupOlderThan(maxAge time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lastAccessTime := time.Now().Add(-maxAge)

	for k, v := range c.requirements {
		if v.lastAccess.Before(lastAccessTime) {
			delete(c.requirements, k)
		}
	}
}
