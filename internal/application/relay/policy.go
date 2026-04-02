package relay

import (
	"math/rand"
	"sync"
)

// StochasticForwardPolicy forwards with probability p (sample in [0,1) < p).
type StochasticForwardPolicy struct {
	P   float64
	RNG *rand.Rand
	mu  sync.Mutex // RNG is not safe for concurrent use
}

func (p *StochasticForwardPolicy) ShouldForward() bool {
	if p.RNG == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.RNG.Float64() < p.P
}
