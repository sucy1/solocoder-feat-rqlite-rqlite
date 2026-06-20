package rlimit

import (
	"sync"
	"time"
)

type Limiter struct {
	rate       float64
	burst      int
	mu         sync.Mutex
	tokens     float64
	lastUpdate time.Time
}

func NewLimiter(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

func (l *Limiter) Allow() bool {
	return l.AllowN(1)
}

func (l *Limiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	l.lastUpdate = now

	if l.tokens >= float64(n) {
		l.tokens -= float64(n)
		return true
	}
	return false
}

type RateLimiter struct {
	rate      float64
	burst     int
	whitelist map[string]bool
	mu        sync.RWMutex
	limiters  map[string]*Limiter
}

func New(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:      rate,
		burst:     burst,
		whitelist: make(map[string]bool),
		limiters:  make(map[string]*Limiter),
	}
}

func (rl *RateLimiter) SetWhitelist(ips []string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.whitelist = make(map[string]bool)
	for _, ip := range ips {
		rl.whitelist[ip] = true
	}
}

func (rl *RateLimiter) IsWhitelisted(ip string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.whitelist[ip]
}

func (rl *RateLimiter) Allow(ip string) bool {
	if rl.rate <= 0 {
		return true
	}

	if rl.IsWhitelisted(ip) {
		return true
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, ok := rl.limiters[ip]
	if !ok {
		limiter = NewLimiter(rl.rate, rl.burst)
		rl.limiters[ip] = limiter
	}
	return limiter.Allow()
}

func (rl *RateLimiter) Stats() map[string]any {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return map[string]any{
		"rate":           rl.rate,
		"burst":          rl.burst,
		"num_ips":        len(rl.limiters),
		"whitelist_size": len(rl.whitelist),
	}
}
