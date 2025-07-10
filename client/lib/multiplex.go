package snowflake_client

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// MultiplexedPacketConn multiplexes packets over a set of underlying PacketConns.
type MultiplexedPacketConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr // All underlying conns should ideally point to the same logical remote endpoint.

	conns []*underlyingConnWrapper
	mu    sync.RWMutex // Protects conns slice

	readChan chan []byte // Channel for incoming packets from all connections
	closed   chan struct{}
	closeMux sync.Once

	// For round-robin sending
	sendIndex uint32
	sendMu    sync.Mutex
}

type underlyingConnWrapper struct {
	pconn    net.PacketConn
	cancel   context.CancelFunc // To stop the read loop for this specific conn
	isClosed bool
	mu       sync.Mutex
}

func (ucw *underlyingConnWrapper) Close() error {
	ucw.mu.Lock()
	defer ucw.mu.Unlock()
	if ucw.isClosed {
		return nil
	}
	ucw.isClosed = true
	if ucw.cancel != nil {
		ucw.cancel()
	}
	return ucw.pconn.Close()
}

func (ucw *underlyingConnWrapper) IsClosed() bool {
	ucw.mu.Lock()
	defer ucw.mu.Unlock()
	return ucw.isClosed
}

// NewMultiplexedPacketConn creates a new MultiplexedPacketConn.
// initialConns can be nil or empty. More conns can be added later.
func NewMultiplexedPacketConn(localAddr, remoteAddr net.Addr, initialConns []net.PacketConn) *MultiplexedPacketConn {
	mpc := &MultiplexedPacketConn{
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
		readChan:   make(chan []byte, 2048), // Buffer size, adjust as needed
		closed:     make(chan struct{}),
		conns:      make([]*underlyingConnWrapper, 0, len(initialConns)),
	}

	for _, pconn := range initialConns {
		mpc.addConn(pconn)
	}

	return mpc
}

func (mpc *MultiplexedPacketConn) addConn(pconn net.PacketConn) {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	if mpc.isClosed() {
		pconn.Close() // Don't add to a closed multiplexer
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	wrapper := &underlyingConnWrapper{
		pconn:  pconn,
		cancel: cancel,
	}
	mpc.conns = append(mpc.conns, wrapper)

	go mpc.readLoop(wrapper, ctx)
	log.Printf("MultiplexedPacketConn: Added new underlying connection. Total: %d", len(mpc.conns))
}

// AddConns adds new PacketConns to the multiplexer.
func (mpc *MultiplexedPacketConn) AddConns(pconns []net.PacketConn) {
	for _, pconn := range pconns {
		mpc.addConn(pconn)
	}
}

func (mpc *MultiplexedPacketConn) readLoop(ucw *underlyingConnWrapper, ctx context.Context) {
	buf := make([]byte, 65536) // Max UDP packet size
	for {
		select {
		case <-ctx.Done(): // Triggered by wrapper.Close() or mpc.Close()
			log.Printf("MultiplexedPacketConn: Read loop for a connection stopping due to context done.")
			return
		case <-mpc.closed:
			log.Printf("MultiplexedPacketConn: Read loop for a connection stopping due to mpc closed.")
			return
		default:
			// Set a deadline to make ReadFrom unblock periodically and check context.
			// This helps in gracefully shutting down the readLoop.
			if err := ucw.pconn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
				// If SetReadDeadline is not supported or fails, log it.
				// The loop might take longer to exit on Close in this case.
				log.Printf("MultiplexedPacketConn: Warning - failed to set read deadline: %v", err)
			}

			n, _, err := ucw.pconn.ReadFrom(buf)
			if err != nil {
				// Check if the error is due to timeout, which is expected.
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Deadline exceeded, loop again to check context/closed status
				}
				// If it's a non-timeout error, or if the connection is definitively closed.
				if !errors.Is(err, net.ErrClosed) && err != io.EOF {
					log.Printf("MultiplexedPacketConn: Error reading from underlying connection: %v", err)
				}
				ucw.Close() // Ensure this conn is marked and fully closed
				mpc.removeConn(ucw)
				return // Stop this read loop
			}
			if n > 0 {
				pkt := make([]byte, n)
				copy(pkt, buf[:n])
				select {
				case mpc.readChan <- pkt:
				case <-mpc.closed:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (mpc *MultiplexedPacketConn) removeConn(ucw *underlyingConnWrapper) {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()
	if mpc.isClosed() {
		return
	}
	newConns := make([]*underlyingConnWrapper, 0, len(mpc.conns))
	found := false
	for _, c := range mpc.conns {
		if c == ucw {
			found = true
			continue
		}
		newConns = append(newConns, c)
	}
	if found {
		mpc.conns = newConns
		log.Printf("MultiplexedPacketConn: Removed underlying connection. Total: %d", len(mpc.conns))
		if len(mpc.conns) == 0 {
			log.Println("MultiplexedPacketConn: All underlying connections removed.")
			// Consider if the multiplexer itself should close or signal an error state here.
			// For now, it will just stop being able to send/receive until new conns are added.
		}
	}
}

// ReadFrom reads a packet from any of the underlying connections.
func (mpc *MultiplexedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	select {
	case <-mpc.closed:
		return 0, nil, net.ErrClosed
	case pkt, ok := <-mpc.readChan:
		if !ok {
			return 0, nil, net.ErrClosed
		}
		if len(pkt) > len(p) {
			return 0, nil, errors.New("buffer too small for packet")
		}
		n = copy(p, pkt)
		return n, mpc.remoteAddr, nil // Assume all conns share the same logical remote addr
	}
}

// WriteTo writes a packet using one of the underlying connections (round-robin).
func (mpc *MultiplexedPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	mpc.sendMu.Lock()
	defer mpc.sendMu.Unlock()

	mpc.mu.RLock()
	if mpc.isClosed() {
		mpc.mu.RUnlock()
		return 0, net.ErrClosed
	}
	if len(mpc.conns) == 0 {
		mpc.mu.RUnlock()
		return 0, errors.New("no underlying connections available")
	}

	// Basic round-robin for sending
	// More sophisticated strategies (e.g., least loaded, fastest) could be implemented.
	numConns := len(mpc.conns)
	var selectedConn *underlyingConnWrapper
	var connErr error

	// Try a few times to find a working connection
	for i := 0; i < numConns; i++ {
		idx := (mpc.sendIndex + uint32(i)) % uint32(numConns)
		connWrapper := mpc.conns[idx]
		mpc.mu.RUnlock() // Unlock while writing

		if connWrapper.IsClosed() {
			mpc.mu.RLock() // Re-lock to continue loop
			continue
		}

		// The remote address for WriteTo on a PacketConn is often ignored if the PacketConn
		// was created by Dialing a specific remote. Here, we assume the underlying
		// PacketConns (like RedialPacketConn) handle their own remote addressing.
		// So, the `addr` parameter might be redundant or could be used for validation if needed.
		n, err = connWrapper.pconn.WriteTo(p, connWrapper.pconn.RemoteAddr())
		mpc.mu.RLock() // Re-lock

		if err != nil {
			log.Printf("MultiplexedPacketConn: Error writing to underlying connection %d: %v. Trying next.", idx, err)
			connErr = err // Store last error
			connWrapper.Close() // Assume this conn is bad
			mpc.removeConn(connWrapper) // removeConn handles its own locking
			// After removeConn, mpc.conns might have changed, so we need to re-evaluate numConns and sendIndex for safety
			// However, for simplicity in this loop, we continue with the old numConns,
			// and the stale conn will be skipped in subsequent iterations or removed.
			// A more robust approach might restart the selection or update numConns.
			numConns = len(mpc.conns) // update numConns after potential removal
			if numConns == 0 {
				break
			}
			continue // Try next connection
		}
		selectedConn = connWrapper
		mpc.sendIndex = (idx + 1) % uint32(numConns) // Move to next for next send
		break
	}
	mpc.mu.RUnlock()


	if selectedConn == nil {
		if connErr != nil {
			return 0, connErr
		}
		return 0, errors.New("no available underlying connection to write to")
	}

	return n, nil
}

// Close closes the MultiplexedPacketConn and all underlying connections.
func (mpc *MultiplexedPacketConn) Close() error {
	mpc.closeMux.Do(func() {
		mpc.mu.Lock() // Acquire lock to safely modify mpc.conns and mpc.closed
		defer mpc.mu.Unlock()

		close(mpc.closed) // Signal all loops to stop
		for _, wrapper := range mpc.conns {
			wrapper.Close() // This will trigger its cancel func and pconn.Close()
		}
		mpc.conns = nil // Clear the slice
		close(mpc.readChan) // Close read channel after ensuring all writers (readLoops) are stopping
		log.Println("MultiplexedPacketConn: Closed.")
	})
	return nil
}

func (mpc *MultiplexedPacketConn) isClosed() bool {
	select {
	case <-mpc.closed:
		return true
	default:
		return false
	}
}

// LocalAddr returns the local network address.
func (mpc *MultiplexedPacketConn) LocalAddr() net.Addr {
	return mpc.localAddr
}

// SetDeadline sets the read and write deadlines associated
// with the connection.
// This is a basic implementation; for per-connection deadlines, more complex logic is needed.
func (mpc *MultiplexedPacketConn) SetDeadline(t time.Time) error {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()
	if mpc.isClosed() {
		return net.ErrClosed
	}
	var err error
	for _, wrapper := range mpc.conns {
		if e := wrapper.pconn.SetDeadline(t); e != nil {
			err = e // Store last error
		}
	}
	return err
}

// SetReadDeadline sets the deadline for future ReadFrom calls.
func (mpc *MultiplexedPacketConn) SetReadDeadline(t time.Time) error {
	// This is tricky for a multiplexed connection.
	// Setting it on underlying conns might not have the intended effect on the mpc.ReadFrom(),
	// which reads from a channel.
	// For now, this is a best-effort pass-through.
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()
	if mpc.isClosed() {
		return net.ErrClosed
	}
	var err error
	for _, wrapper := range mpc.conns {
		if e := wrapper.pconn.SetReadDeadline(t); e != nil {
			err = e
		}
	}
	return err
}

// SetWriteDeadline sets the deadline for future WriteTo calls.
func (mpc *MultiplexedPacketConn) SetWriteDeadline(t time.Time) error {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()
	if mpc.isClosed() {
		return net.ErrClosed
	}
	var err error
	for _, wrapper := range mpc.conns {
		if e := wrapper.pconn.SetWriteDeadline(t); e != nil {
			err = e
		}
	}
	return err
}

// RemoteAddr returns the remote network address.
// This assumes all underlying connections share the same logical remote endpoint.
func (mpc *MultiplexedPacketConn) RemoteAddr() net.Addr {
	return mpc.remoteAddr
}
