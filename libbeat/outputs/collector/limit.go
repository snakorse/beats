package collector

import (
	"sync"
	"time"
)

type limiter struct {
	lock sync.Mutex

	quantity  int64
	capacity  int64
	threshold int64
	timeout   time.Duration
}

func newLimiter(quantity, threshold int64, timeout time.Duration) *limiter {
	l := &limiter{
		quantity:  quantity,
		capacity:  threshold,
		threshold: threshold,
		timeout:   timeout,
	}

	go l.supplement()
	return l
}

func (l *limiter) assign(size int64) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.capacity <= 0 {
		return false
	}
	if size > l.capacity {
		return false
	}

	l.capacity -= size
	return true
}

func (l *limiter) fill(size int64) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.capacity += size
	if l.capacity >= l.threshold {
		l.capacity = l.threshold
	}
}

func (l *limiter) supplement() {
	for {
		time.Sleep(l.timeout)

		l.fill(l.quantity)
	}
}
