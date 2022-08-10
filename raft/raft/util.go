package raft

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	// Ensure we use a high-entropy seed for the pseudo-random generator
	rand.Seed(newSeed())
}

// returns an int64 from a crypto random source
// can be used to seed a source for a math/rand.
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := time.Duration(rand.Int63()) % minVal
	return time.After(minVal + extra)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID is used to generate a random UUID.
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

func marshalInterface(command interface{}) []byte {
	b, err := json.Marshal(command)
	if err != nil {
		panic(fmt.Errorf("failed to convert interface{} to []byte: %v", err))
	}
	return b
}

func unMarshlInterface(commandData []byte) interface{} {
	var command interface{}
	err := json.Unmarshal(commandData, &command)
	if err != nil {
		panic(fmt.Errorf("Unmarshal command error: %v", err))
	}
	return command
}

// Needed for sorting []uint64, used to determine commitment
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
