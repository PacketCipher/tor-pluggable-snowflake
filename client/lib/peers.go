package snowflake_client

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"sync"
)

// Peers is a container that keeps track of multiple WebRTC remote peers.
// Implements |SnowflakeCollector|.
//
// Maintaining a set of pre-connected Peers with fresh but inactive datachannels
// allows allows rapid recovery when the current WebRTC Peer disconnects.
//
// This version is adapted to collect and provide multiple peers for concurrent use.
type Peers struct {
	Tongue
	bytesLogger bytesLogger

	// snowflakeChan sends slices of WebRTCPeer for concurrent use.
	snowflakeChan chan []*WebRTCPeer
	activePeers   *list.List

	melt chan struct{}

	collectLock sync.Mutex
	closeOnce   sync.Once
}

// NewPeers constructs a fresh container of remote peers.
func NewPeers(tongue Tongue) (*Peers, error) {
	p := &Peers{}
	if tongue == nil {
		return nil, errors.New("missing Tongue to catch Snowflakes with")
	}
	// The channel buffer should be related to how many sets of peers we might pre-fetch.
	// For now, a small buffer is likely fine. GetMax() refers to total pool size.
	// GetSimultaneous() refers to how many are used at once.
	// The channel will send a slice of peers.
	p.snowflakeChan = make(chan []*WebRTCPeer, tongue.GetMax()/tongue.GetSimultaneous()+1)
	p.activePeers = list.New()
	p.melt = make(chan struct{})
	p.Tongue = tongue
	return p, nil
}

// Collect connects to and adds a new set of remote peers as part of |SnowflakeCollector| interface.
// It attempts to collect `tongue.GetSimultaneous()` peers.
func (p *Peers) Collect() ([]*WebRTCPeer, error) {
	p.collectLock.Lock()
	defer p.collectLock.Unlock()

	select {
	case <-p.melt:
		return nil, fmt.Errorf("Snowflakes have melted")
	default:
	}

	if nil == p.Tongue {
		return nil, errors.New("missing Tongue to catch Snowflakes with")
	}

	numToCollect := p.Tongue.GetSimultaneous()
	if numToCollect <= 0 {
		numToCollect = 1 // Should always collect at least one.
	}
	capacity := p.Tongue.GetMax()
	currentCount := p.Count()

	if currentCount >= capacity {
		return nil, fmt.Errorf("At or over pool capacity [%d/%d]", currentCount, capacity)
	}
	// Adjust numToCollect if we're close to capacity
	if currentCount+numToCollect > capacity {
		numToCollect = capacity - currentCount
	}

	if numToCollect == 0 { // Should not happen if capacity checks are correct
		return nil, fmt.Errorf("Calculated zero peers to collect, current: %d, capacity: %d", currentCount, capacity)
	}

	log.Printf("WebRTC: Collecting %d new Snowflake(s). Current pool: [%d/%d]", numToCollect, currentCount, capacity)

	collectedPeers := make([]*WebRTCPeer, 0, numToCollect)
	var firstError error

	for i := 0; i < numToCollect; i++ {
		// Check melt signal inside the loop in case collection takes time
		select {
		case <-p.melt:
			if len(collectedPeers) > 0 {
				// If we collected some peers before melting, pass them along.
				p.snowflakeChan <- collectedPeers
			}
			return nil, fmt.Errorf("Snowflakes have melted during collection")
		default:
		}

		connection, err := p.Tongue.Catch()
		if nil != err {
			log.Printf("Error collecting a snowflake: %v. Collected %d so far in this batch.", err, len(collectedPeers))
			if firstError == nil {
				firstError = err
			}
			// If we fail to get one, we might not be able to get more.
			// Break and try to use what we have, or report error if none.
			break
		}
		p.activePeers.PushBack(connection)
		collectedPeers = append(collectedPeers, connection)
	}

	if len(collectedPeers) == 0 {
		if firstError != nil {
			return nil, fmt.Errorf("failed to collect any snowflakes in batch: %w", firstError)
		}
		return nil, errors.New("failed to collect any snowflakes in batch, no specific error")
	}

	log.Printf("WebRTC: Successfully collected %d snowflake(s). Passing to channel.", len(collectedPeers))
	p.snowflakeChan <- collectedPeers
	return collectedPeers, nil
}

// Pop blocks until an available, valid set of snowflakes appears.
// Pop will return nil after End has been called.
func (p *Peers) Pop() []*WebRTCPeer {
	for {
		snowflakes, ok := <-p.snowflakeChan
		if !ok {
			// Channel closed, likely due to End() being called.
			return nil
		}

		validPeers := make([]*WebRTCPeer, 0, len(snowflakes))
		for _, snowflake := range snowflakes {
			if snowflake != nil && !snowflake.Closed() {
				// Set to use the same rate-limited traffic logger to keep consistency.
				snowflake.bytesLogger = p.bytesLogger
				validPeers = append(validPeers, snowflake)
			}
		}

		if len(validPeers) > 0 {
			return validPeers
		}
		// If all peers in the fetched set were closed, loop again to get a fresh set.
		log.Println("Popped a set of snowflakes, but all were closed or invalid. Retrying.")
	}
}

// Melted returns a channel that will close when peers stop being collected.
// Melted is a necessary part of |SnowflakeCollector| interface.
func (p *Peers) Melted() <-chan struct{} {
	return p.melt
}

// isMelted returns true if the peer collector has been stopped.
func (p *Peers) isMelted() bool {
	select {
	case <-p.melt:
		return true
	default:
		return false
	}
}

// Count returns the total available Snowflakes (including the active ones)
// The count only reduces when connections themselves close, rather than when
// they are popped.
func (p *Peers) Count() int {
	p.collectLock.Lock() // Protect access to activePeers
	defer p.collectLock.Unlock()
	p.purgeClosedPeers() // purgeClosedPeers is now called under lock
	return p.activePeers.Len()
}

// purgeClosedPeers MUST be called with p.collectLock held.
func (p *Peers) purgeClosedPeers() {
	for e := p.activePeers.Front(); e != nil; {
		next := e.Next()
		conn := e.Value.(*WebRTCPeer)
		// Purge those marked for deletion.
		if conn.Closed() {
			p.activePeers.Remove(e)
		}
		e = next
	}
}

// End closes all active connections to Peers contained here, and stops the
// collection of future Peers.
func (p *Peers) End() {
	p.closeOnce.Do(func() {
		close(p.melt)
		p.collectLock.Lock()
		defer p.collectLock.Unlock()
		close(p.snowflakeChan)
		cnt := p.Count()
		for e := p.activePeers.Front(); e != nil; {
			next := e.Next()
			conn := e.Value.(*WebRTCPeer)
			conn.Close()
			p.activePeers.Remove(e)
			e = next
		}
		log.Printf("WebRTC: melted all %d snowflakes.", cnt)
	})
}
