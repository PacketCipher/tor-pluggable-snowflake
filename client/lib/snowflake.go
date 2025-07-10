/*
Package snowflake_client implements functionality necessary for a client to establish a connection
to a server using Snowflake.

Included in the package is a Transport type that implements the Pluggable Transports v2.1 Go API
specification. To use Snowflake, you must first create a client from a configuration:

	config := snowflake_client.ClientConfig{
		BrokerURL:   "https://snowflake-broker.example.com",
		FrontDomain: "https://friendlyfrontdomain.net",
		// ...
	}
	transport, err := snowflake_client.NewSnowflakeClient(config)
	if err != nil {
		// handle error
	}

The Dial function connects to a Snowflake server:

	conn, err := transport.Dial()
	if err != nil {
		// handle error
	}
	defer conn.Close()
*/
package snowflake_client

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/xtaci/kcp-go/v5"
	"github.com/xtaci/smux"

	"gitlab.torproject.org/tpo/anti-censorship/pluggable-transports/snowflake/v2/common/event"
	"gitlab.torproject.org/tpo/anti-censorship/pluggable-transports/snowflake/v2/common/nat"
	"gitlab.torproject.org/tpo/anti-censorship/pluggable-transports/snowflake/v2/common/turbotunnel"
)

const (
	// ReconnectTimeout is the time a Snowflake client will wait before collecting
	// more snowflakes.
	ReconnectTimeout = 10 * time.Second
	// SnowflakeTimeout is the time a Snowflake client will wait before determining that
	// a remote snowflake has been disconnected. If no new messages are sent or received
	// in this time period, the client will terminate the connection with the remote
	// peer and collect a new snowflake.
	SnowflakeTimeout = 20 * time.Second
	// DataChannelTimeout is how long the client will wait for the OnOpen callback
	// on a newly created DataChannel.
	DataChannelTimeout = 10 * time.Second

	// WindowSize is the number of packets in the send and receive window of a KCP connection.
	WindowSize = 65535
	// StreamSize controls the maximum amount of in flight data between a client and server.
	StreamSize = 1048576 // 1MB
)

type dummyAddr struct{}

func (addr dummyAddr) Network() string { return "dummy" }
func (addr dummyAddr) String() string  { return "dummy" }

// Transport is a structure with methods that conform to the Go PT v2.1 API
// https://github.com/Pluggable-Transports/Pluggable-Transports-spec/blob/master/releases/PTSpecV2.1/Pluggable%20Transport%20Specification%20v2.1%20-%20Go%20Transport%20API.pdf
type Transport struct {
	dialer *WebRTCDialer

	// EventDispatcher is the event bus for snowflake events.
	// When an important event happens, it will be distributed here.
	eventDispatcher event.SnowflakeEventDispatcher
}

// ClientConfig defines how the SnowflakeClient will connect to the broker and Snowflake proxies.
type ClientConfig struct {
	// BrokerURL is the full URL of the Snowflake broker that the client will connect to.
	BrokerURL string
	// AmpCacheURL is the full URL of a valid AMP cache. A nonzero value indicates
	// that AMP cache will be used as the rendezvous method with the broker.
	AmpCacheURL string
	// SQSQueueURL is the full URL of an AWS SQS Queue. A nonzero value indicates
	// that SQS queue will be used as the rendezvous method with the broker.
	SQSQueueURL string
	// Base64 encoded string of the credentials containing access Key ID and secret key used to access the AWS SQS Qeueue
	SQSCredsStr string
	// FrontDomain is the full URL of an optional front domain that can be used with either
	// the AMP cache or HTTP domain fronting rendezvous method.
	FrontDomain string
	// ICEAddresses are a slice of ICE server URLs that will be used for NAT traversal and
	// the creation of the client's WebRTC SDP offer.
	FrontDomains []string
	// ICEAddresses are a slice of ICE server URLs that will be used for NAT traversal and
	// the creation of the client's WebRTC SDP offer.
	ICEAddresses []string
	// KeepLocalAddresses is an optional setting that will prevent the removal of local or
	// invalid addresses from the client's SDP offer. This is useful for local deployments
	// and testing.
	KeepLocalAddresses bool
	// Max is the maximum number of snowflake proxy peers that the client should attempt to
	// connect to. Defaults to 1. This will represent the total pool size.
	Max int
	// SimultaneousProxies is the number of proxies to use concurrently for multiplexing.
	// Defaults to 1 if not specified or if less than 1.
	SimultaneousProxies int
	// UTLSClientID is the type of user application that snowflake should imitate.
	// If an empty value is provided, it will use Go's default TLS implementation
	UTLSClientID string
	// UTLSRemoveSNI is the flag to control whether SNI should be removed from Client Hello
	// when uTLS is used.
	UTLSRemoveSNI bool
	// BridgeFingerprint is the fingerprint of the bridge that the client will eventually
	// connect to, as specified in the Bridge line of the torrc.
	BridgeFingerprint string
	// CommunicationProxy is the proxy address for network communication
	CommunicationProxy *url.URL
}

// NewSnowflakeClient creates a new Snowflake transport client that can spawn multiple
// Snowflake connections.
//
// brokerURL and frontDomain are the urls for the broker host and domain fronting host
// iceAddresses are the STUN/TURN urls needed for WebRTC negotiation
// keepLocalAddresses is a flag to enable sending local network addresses (for testing purposes)
// max is the maximum number of snowflakes the client should gather for each SOCKS connection
func NewSnowflakeClient(config ClientConfig) (*Transport, error) {
	log.Println("\n\n\n --- Starting Snowflake Client ---")

	iceServers := parseIceServers(config.ICEAddresses)
	// chooses a random subset of servers from inputs
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(iceServers), func(i, j int) {
		iceServers[i], iceServers[j] = iceServers[j], iceServers[i]
	})
	if len(iceServers) > 2 {
		iceServers = iceServers[:(len(iceServers)+1)/2]
	}
	log.Printf("Using ICE servers:")
	for _, server := range iceServers {
		log.Printf("url: %v", strings.Join(server.URLs, " "))
	}

	// Maintain backwards compatibility with old FrontDomain field of ClientConfig
	if (len(config.FrontDomains) == 0) && (config.FrontDomain != "") {
		config.FrontDomains = []string{config.FrontDomain}
	}

	// Rendezvous with broker using the given parameters.
	broker, err := newBrokerChannelFromConfig(config)
	if err != nil {
		return nil, err
	}
	go updateNATType(iceServers, broker, config.CommunicationProxy)

	natPolicy := &NATPolicy{}

	max := 1
	if config.Max > max {
		max = config.Max
	}
	simultaneousProxies := 1
	if config.SimultaneousProxies > simultaneousProxies {
		simultaneousProxies = config.SimultaneousProxies
	}
	// Ensure Max is at least as large as SimultaneousProxies
	if max < simultaneousProxies {
		max = simultaneousProxies
	}

	eventsLogger := event.NewSnowflakeEventDispatcher()
	// Pass simultaneousProxies to the WebRTCDialer constructor.
	// Note: We'll need to modify NewWebRTCDialerWithNatPolicyAndEventsAndProxy next to accept this.
	transport := &Transport{dialer: NewWebRTCDialerWithNatPolicyAndEventsAndProxy(broker, natPolicy, iceServers, max, simultaneousProxies, eventsLogger, config.CommunicationProxy), eventDispatcher: eventsLogger}

	return transport, nil
}

// Dial creates a new Snowflake connection.
// Dial starts the collection of snowflakes and returns a SnowflakeConn that is a
// wrapper around a smux.Stream that will reliably deliver data to a Snowflake
// server through one or more snowflake proxies.
func (t *Transport) Dial() (net.Conn, error) {
	// Cleanup functions to run before returning, in case of an error.
	var cleanup []func()
	defer func() {
		// Run cleanup in reverse order, as defer does.
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}()

	// Prepare to collect remote WebRTC peers.
	snowflakes, err := NewPeers(t.dialer)
	if err != nil {
		return nil, err
	}
	cleanup = append(cleanup, func() { snowflakes.End() })

	// Use a real logger to periodically output how much traffic is happening.
	snowflakes.bytesLogger = newBytesSyncLogger()

	log.Printf("---- SnowflakeConn: begin collecting snowflakes ---")
	go connectLoop(snowflakes)

	// Create a new smux session
	log.Printf("---- SnowflakeConn: starting a new session ---")
	pconn, sess, err := newSession(snowflakes)
	if err != nil {
		return nil, err
	}
	cleanup = append(cleanup, func() {
		pconn.Close()
		sess.Close()
	})

	// On the smux session we overlay a stream.
	stream, err := sess.OpenStream()
	if err != nil {
		return nil, err
	}
	// Begin exchanging data.
	log.Printf("---- SnowflakeConn: begin stream %v ---", stream.ID())
	cleanup = append(cleanup, func() { stream.Close() })

	// All good, clear the cleanup list.
	cleanup = nil
	return &SnowflakeConn{Stream: stream, sess: sess, pconn: pconn, snowflakes: snowflakes}, nil
}

func (t *Transport) AddSnowflakeEventListener(receiver event.SnowflakeEventReceiver) {
	t.eventDispatcher.AddSnowflakeEventListener(receiver)
}

func (t *Transport) RemoveSnowflakeEventListener(receiver event.SnowflakeEventReceiver) {
	t.eventDispatcher.RemoveSnowflakeEventListener(receiver)
}

// SetRendezvousMethod sets the rendezvous method to the Snowflake broker.
func (t *Transport) SetRendezvousMethod(r RendezvousMethod) {
	t.dialer.Rendezvous = r
}

// SnowflakeConn is a reliable connection to a snowflake server that implements net.Conn.
type SnowflakeConn struct {
	*smux.Stream
	sess       *smux.Session
	pconn      net.PacketConn
	snowflakes *Peers
}

// Close closes the connection.
//
// The collection of snowflake proxies for this connection is stopped.
func (conn *SnowflakeConn) Close() error {
	var err error
	log.Printf("---- SnowflakeConn: closed stream %v ---", conn.ID())
	err = conn.Stream.Close()
	log.Printf("---- SnowflakeConn: end collecting snowflakes ---")
	conn.snowflakes.End()
	if inerr := conn.pconn.Close(); err == nil {
		err = inerr
	}
	log.Printf("---- SnowflakeConn: discarding finished session ---")
	if inerr := conn.sess.Close(); err == nil {
		err = inerr
	}
	return err
}

// loop through all provided STUN servers until we exhaust the list or find
// one that is compatible with RFC 5780
func updateNATType(servers []webrtc.ICEServer, broker *BrokerChannel, proxy *url.URL) {
	var restrictedNAT bool
	var err error
	for _, server := range servers {
		addr := strings.TrimPrefix(server.URLs[0], "stun:")
		restrictedNAT, err = nat.CheckIfRestrictedNATWithProxy(addr, proxy)

		if err != nil {
			log.Printf("Warning: NAT checking failed for server at %s: %s", addr, err)
		} else {
			if restrictedNAT {
				broker.SetNATType(nat.NATRestricted)
			} else {
				broker.SetNATType(nat.NATUnrestricted)
			}
			break
		}
	}
	if err != nil {
		broker.SetNATType(nat.NATUnknown)
	}
}

// Returns a slice of webrtc.ICEServer given a slice of addresses
func parseIceServers(addresses []string) []webrtc.ICEServer {
	var servers []webrtc.ICEServer
	if len(addresses) == 0 {
		return nil
	}
	for _, address := range addresses {
		address = strings.TrimSpace(address)

		// ice.ParseURL recognizes many types of ICE servers,
		// but we only support stun over UDP currently
		u, err := url.Parse(address)
		if err != nil {
			log.Printf("Warning: Parsing ICE server %v resulted in error: %v, skipping", address, err)
			continue
		}
		if u.Scheme != "stun" {
			log.Printf("Warning: Only stun: (STUN over UDP) servers are supported currently, skipping %v", address)
			continue
		}

		// add default port, other sanity checks
		parsedURL, err := ice.ParseURL(address)
		if err != nil {
			log.Printf("Warning: Parsing ICE server %v resulted in error: %v, skipping", address, err)
			continue
		}

		servers = append(servers, webrtc.ICEServer{
			URLs: []string{parsedURL.String()},
		})
	}
	return servers
}

// newSession returns a new smux.Session and the net.PacketConn it is running
// over. The net.PacketConn will multiplex over several Snowflake proxies
// pulled from the snowflakes collector.
func newSession(snowflakes *Peers) (net.PacketConn, *smux.Session, error) {
	clientID := turbotunnel.NewClientID()

	initialPeers := snowflakes.Pop()
	if len(initialPeers) == 0 {
		return nil, nil, errors.New("newSession: failed to get initial set of snowflake peers")
	}
	log.Printf("newSession: Got initial %d peers.", len(initialPeers))

	underlyingPacketConns := make([]net.PacketConn, 0, len(initialPeers))
	for _, peer := range initialPeers {
		// Need a unique dialContext for each RedialPacketConn if they are to redial independently.
		// However, the current RedialPacketConn is designed for a single conceptual "pipe"
		// that finds new endpoints. If we want each of N connections to redial independently
		// from the shared pool, this gets more complex.

		// For now, let's simplify: each RedialPacketConn created here will try to redial
		// by popping a *single* peer from the main `snowflakes` collector.
		// This means the `snowflakes.Pop()` needs to be safe for concurrent calls if redials happen concurrently.
		// The `Peers.Pop()` now returns a slice. This dialContext will need to handle that,
		// perhaps by taking one from the slice and putting others back, or using one and discarding others for this specific redial.
		// This part needs careful thought.

		// Simpler approach for now: The MultiplexedPacketConn will manage a set of RedialPacketConns.
		// Each RedialPacketConn, when it needs a new underlying WebRTC conn, will ask the `snowflakes` collector.
		// The `snowflakes.Pop()` returns a slice. We need a mechanism to distribute these new peers
		// to the RedialPacketConns that need them, or to create new RedialPacketConns.

		// Let's make each RedialPacketConn responsible for its own lifecycle.
		// The dialContext will pop a *single* peer.
		// We need a way for `snowflakes` to provide single peers for redialing,
		// or adapt RedialPacketConn to take a slice and manage it.

		// Temporary: We'll create one RedialPacketConn for each initial peer.
		// The `dialContext` for each will attempt to pop a *new set* and take one,
		// which is inefficient. This part of the logic (managing redials for multiple paths)
		// is the most complex.

		currentPeer := peer // Capture for the closure
		dialContext := func(ctx context.Context) (net.PacketConn, error) {
			log.Printf("Redialing for one of the multiplexed connections.")
			// This Pop will get a new *set* of peers. We only need one for this redial.
			// This is not ideal. The Peers structure might need a way to provide single peers for redialing,
			// or a way to replenish individual failed connections within the multiplexer.
			newPeerSet := snowflakes.Pop()
			if len(newPeerSet) == 0 {
				return nil, errors.New("redial: failed to get a new snowflake peer")
			}
			// Use the first one for this redial. What about the rest?
			// They are effectively popped from the collector. This could starve other paths if not handled.
			// For now, we'll use the first and log if others were available.
			selectedPeer := newPeerSet[0]
			if len(newPeerSet) > 1 {
				log.Printf("Redial: Popped %d peers, using one. Others are currently discarded in this redial logic.", len(newPeerSet))
				// Ideally, unused peers from newPeerSet should be offered back to a pool or used to replace other failed conns.
			}
			currentPeer = selectedPeer // Update currentPeer for this path for next potential operations within this dialContext instance.

			log.Printf("---- Handler: snowflake %s assigned for redial ----", currentPeer.ID())
			_, err := currentPeer.Write(turbotunnel.Token[:])
			if err != nil {
				currentPeer.Close()
				return nil, fmt.Errorf("redial write token: %w", err)
			}
			_, err = currentPeer.Write(clientID[:])
			if err != nil {
				currentPeer.Close()
				return nil, fmt.Errorf("redial write clientID: %w", err)
			}
			return newEncapsulationPacketConn(dummyAddr{}, dummyAddr{}, currentPeer), nil
		}

		// Initial connection setup for this path
		log.Printf("---- Handler: snowflake %s assigned for initial connection ----", currentPeer.ID())
		_, err := currentPeer.Write(turbotunnel.Token[:])
		if err != nil {
			log.Printf("Failed to write token to initial peer %s: %v", currentPeer.ID(), err)
			currentPeer.Close() // Close this peer
			continue            // Skip this peer
		}
		_, err = currentPeer.Write(clientID[:])
		if err != nil {
			log.Printf("Failed to write clientID to initial peer %s: %v", currentPeer.ID(), err)
			currentPeer.Close()
			continue
		}
		encapConn := newEncapsulationPacketConn(dummyAddr{}, dummyAddr{}, currentPeer)

		// Each RedialPacketConn represents one path in the multiplexer.
		// It starts with the encapConn from the initial peer.
		// If that path breaks, its dialContext is called to get a new encapConn.
		redialPConn := turbotunnel.NewRedialPacketConnWithInitial(dummyAddr{}, dummyAddr{}, encapConn, dialContext)
		underlyingPacketConns = append(underlyingPacketConns, redialPConn)
	}

	if len(underlyingPacketConns) == 0 {
		return nil, nil, errors.New("newSession: failed to establish any initial underlying connections")
	}

	log.Printf("newSession: Created %d underlying RedialPacketConns.", len(underlyingPacketConns))

	// Create the multiplexer on top of these RedialPacketConns
	multiplexedPConn := NewMultiplexedPacketConn(dummyAddr{}, dummyAddr{}, underlyingPacketConns)

	// The KCP connection runs over the multiplexed connection.
	conn, err := kcp.NewConn2(dummyAddr{}, nil, 0, 0, multiplexedPConn)
	if err != nil {
		multiplexedPConn.Close()
		return nil, nil, fmt.Errorf("kcp.NewConn2 failed: %w", err)
	}

	conn.SetStreamMode(true)
	conn.SetWindowSize(WindowSize, WindowSize)
	conn.SetNoDelay(0, 0, 0, 1) // nc=1 => congestion window off

	smuxConfig := smux.DefaultConfig()
	smuxConfig.Version = 2
	smuxConfig.KeepAliveTimeout = 10 * time.Minute // Or SnowflakeTimeout?
	smuxConfig.MaxStreamBuffer = StreamSize

	sess, err := smux.Client(conn, smuxConfig)
	if err != nil {
		conn.Close()
		multiplexedPConn.Close() // This will close all underlying RedialPacketConns
		return nil, nil, fmt.Errorf("smux.Client failed: %w", err)
	}

	// Start a goroutine to replenish connections in the multiplexer if they drop.
	// This is a basic replenishment strategy.
	go replenishConnections(multiplexedPConn, snowflakes, clientID, snowflakes.Tongue.GetSimultaneous())

	return multiplexedPConn, sess, nil
}


// replenishConnections tries to keep the number of active connections in the multiplexer
// up to the desired number of simultaneous proxies.
func replenishConnections(mpc *MultiplexedPacketConn, snowflakes *Peers, clientID turbotunnel.ClientID, desiredConns int) {
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-mpc.closed:
			log.Println("Replenish connections loop stopping as multiplexer is closed.")
			return
		case <-snowflakes.Melted():
			log.Println("Replenish connections loop stopping as snowflake collector is melted.")
			return
		case <-logTicker.C:
			mpc.mu.RLock()
			numCurrentConns := len(mpc.conns)
			mpc.mu.RUnlock()
			log.Printf("Multiplexer status: %d active connections (target: %d).", numCurrentConns, desiredConns)
		default:
			// Check and replenish
		}

		time.Sleep(5 * time.Second) // Check interval

		mpc.mu.RLock()
		numCurrentConns := len(mpc.conns)
		mpc.mu.RUnlock()

		if mpc.isClosed() || snowflakes.isMelted() { // snowflakes.isMelted() would need to be added to Peers
			return
		}

		needed := desiredConns - numCurrentConns
		if needed <= 0 {
			continue
		}

		log.Printf("Replenishing: need %d more connections for multiplexer (current: %d, desired: %d)", needed, numCurrentConns, desiredConns)

		// Request new peers. Pop() returns a slice.
		newPeerSet := snowflakes.Pop()
		if len(newPeerSet) == 0 {
			log.Println("Replenish: failed to get new peers from collector.")
			time.Sleep(ReconnectTimeout) // Wait before retrying to get peers
			continue
		}

		log.Printf("Replenish: Got %d new peers from collector.", len(newPeerSet))

		var connsToAdd []net.PacketConn
		for i := 0; i < needed && i < len(newPeerSet); i++ {
			peer := newPeerSet[i]
			log.Printf("Replenish: Attempting to use new peer %s", peer.ID())

			// Setup this new peer as a RedialPacketConn
			dialContext := func(ctx context.Context) (net.PacketConn, error) {
				// This dialContext is similar to the one in newSession.
				// It's for when *this specific* RedialPacketConn needs to redial.
				log.Printf("Redialing for a replenished connection.")
				poppedSet := snowflakes.Pop()
				if len(poppedSet) == 0 {
					return nil, errors.New("replenish redial: failed to get a new snowflake peer")
				}
				selectedRedialPeer := poppedSet[0]
				// Again, other peers in poppedSet are currently lost to this specific redial path.
				log.Printf("---- Handler: snowflake %s assigned for replenish redial ----", selectedRedialPeer.ID())

				_, err := selectedRedialPeer.Write(turbotunnel.Token[:])
				if err != nil {
					selectedRedialPeer.Close()
					return nil, fmt.Errorf("replenish redial write token: %w", err)
				}
				_, err = selectedRedialPeer.Write(clientID[:])
				if err != nil {
					selectedRedialPeer.Close()
					return nil, fmt.Errorf("replenish redial write clientID: %w", err)
				}
				return newEncapsulationPacketConn(dummyAddr{}, dummyAddr{}, selectedRedialPeer), nil
			}

			// Initial connection for the new path
			_, err := peer.Write(turbotunnel.Token[:])
			if err != nil {
				log.Printf("Replenish: Failed to write token to new peer %s: %v", peer.ID(), err)
				peer.Close()
				continue
			}
			_, err = peer.Write(clientID[:])
			if err != nil {
				log.Printf("Replenish: Failed to write clientID to new peer %s: %v", peer.ID(), err)
				peer.Close()
				continue
			}
			encapConn := newEncapsulationPacketConn(dummyAddr{}, dummyAddr{}, peer)
			redialPConn := turbotunnel.NewRedialPacketConnWithInitial(dummyAddr{}, dummyAddr{}, encapConn, dialContext)
			connsToAdd = append(connsToAdd, redialPConn)
			log.Printf("Replenish: Successfully prepared new RedialPacketConn with peer %s", peer.ID())
		}

		if len(connsToAdd) > 0 {
			mpc.AddConns(connsToAdd)
			log.Printf("Replenish: Added %d new connections to multiplexer.", len(connsToAdd))
		}
		// If newPeerSet had more peers than needed, they are currently discarded.
		// A more advanced system could put them back into `snowflakes` or a temporary holding.
	}
}


// Maintain enough available WebRTC connections (the pool for `Peers.Collect`)
// This loop calls `snowflakes.Collect()` which tries to get `GetSimultaneous()` peers
// and adds them to the `snowflakeChan` within `Peers`.
// The `Pop()` method in `Peers` then draws from this channel.
func connectLoop(snowflakes *Peers) {
	for {
		// How many do we need to fill the pool up to snowflakes.Tongue.GetMax()?
		currentPeerCount := snowflakes.Count()
		maxPoolSize := snowflakes.Tongue.GetMax() // Total capacity of the peer pool

		if currentPeerCount >= maxPoolSize {
			log.Printf("ConnectLoop: Peer pool is full [%d/%d]. Sleeping.", currentPeerCount, maxPoolSize)
			time.Sleep(ReconnectTimeout) // Pool is full, check again later
			select {
			case <-snowflakes.Melted():
				log.Println("ConnectLoop: stopped (pool full, then melted).")
				return
			default:
				continue
			}
		}

		// We want to collect a batch of peers if the pool isn't full.
		// snowflakes.Collect() will try to get `GetSimultaneous()` peers.
		log.Printf("ConnectLoop: Current pool [%d/%d]. Attempting to collect more peers.", currentPeerCount, maxPoolSize)
		collected, err := snowflakes.Collect() // Collect a batch
		if err != nil {
			log.Printf("ConnectLoop: Error collecting snowflakes: %v. Retrying after timeout.", err)
			// Wait before trying to collect again, especially if errors are persistent
			select {
			case <-time.After(ReconnectTimeout):
			case <-snowflakes.Melted():
				log.Println("ConnectLoop: stopped (error, then melted).")
				return
			}
			continue
		}
		if collected == nil || len(collected) == 0 {
			log.Println("ConnectLoop: Collect returned no peers and no error. Retrying after timeout.")
			select {
			case <-time.After(ReconnectTimeout):
			case <-snowflakes.Melted():
				log.Println("ConnectLoop: stopped (no peers collected, then melted).")
				return
			}
			continue
		}

		log.Printf("ConnectLoop: Successfully collected %d peers. Pool at approx [%d/%d].", len(collected), snowflakes.Count(), maxPoolSize)

		// No explicit timer needed here if Collect() is blocking or has its own timeout logic.
		// The loop naturally waits for Collect() or sleeps if pool is full.
		// We do need to check for the melt signal.
		select {
		case <-snowflakes.Melted():
			log.Println("ConnectLoop: stopped.")
			return
		default:
			// If the pool is not yet full, loop again to collect more.
			// If it is full, the check at the beginning of the loop will cause a sleep.
			// Add a small delay to prevent busy-looping if Collect is very fast and pool isn't filling.
			time.Sleep(1 * time.Second)
		}
	}
}
