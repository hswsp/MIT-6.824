package raft

import (
	"fmt"
	"io"
	"os"
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
}

// DefaultConfig returns a Config with usable defaults.
//The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
//Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds.
//the tester here limits you to 10 heartbeats per second,
//you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
// but not too large, because then you may fail to elect a leader within five seconds.
// two recommended parameter settings:
// a. ElectionTimeout: 150ms-300ms, HeartbeatTimeout: 50ms
// b. ElectionTimeout: 200ms-400ms, HeartbeatTimeout: 100ms
// ref: https://github.com/springfieldking/mit-6.824-golabs-2018/issues/1
func DefaultConfig() *Config {
	id := generateUUID()
	file, err := os.Create("./debug.log")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return &Config{
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		LogLevel:           "DEBUG",
		LocalID:            ServerID(id),
		LogOutput:          file,
	}
}