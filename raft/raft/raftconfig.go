package raft

import (
	"github.com/hashicorp/go-hclog"
	"io"
	"time"
)
// ServerID is a unique string identifying a server for all time.
type ServerID string

// Config provides any necessary configuration for the Raft server.
type Config struct {

	// HeartbeatTimeout specifies the time in follower state without contact
	// from a leader before we attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without contact
	// from a leader before we attempt an election.
	ElectionTimeout time.Duration

	// CommitTimeout specifies the time without an Apply operation before the
	// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
	// log entries.
	// Due to random staggering, may be delayed as much as 2x this value.
	CommitTimeout time.Duration

	// LeaderLeaseTimeout is used to control how long the "lease" lasts
	// for being the leader without being able to contact a quorum
	// of nodes. If we reach this interval without contact, we will
	// step down as leader.
	LeaderLeaseTimeout time.Duration


	// LocalID is a unique ID for this server across all time. When running with
	// ProtocolVersion < 3, you must set this to be the same as the network
	// address of your transport.
	LocalID ServerID

	// LogOutput is used as a sink for logs, unless Logger is specified.
	// Defaults to os.Stderr.
	LogOutput io.Writer

	// LogLevel represents a log level. If the value does not match a known
	// logging level hclog.NoLevel is used.
	LogLevel string

	// Logger is a user-provided logger. If nil, a logger writing to
	// LogOutput with LogLevel is used.
	Logger hclog.Logger
}

// DefaultConfig returns a Config with usable defaults.
func DefaultConfig() *Config {
	return &Config{

		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		LogLevel:           "DEBUG",
	}
}