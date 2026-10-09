package transport

import (
	"io"
	"log"
	"os"
)

// captureLog redirects the standard logger into w for the duration of a test and
// returns a restore func. Used by the robustness specs to assert that transient
// backend failures are actually surfaced to operators.
func captureLog(w io.Writer) func() {
	prevOut := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(w)
	log.SetFlags(0)
	return func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	}
}

// encodeTestFile builds a read-direction payload exactly as the write path would
// have written it for the given sequence number: encoded payload, encrypted when
// the connection has a cipher. Lets read-path specs plant real files on a fake
// store instead of hand-crafting bytes.
func (c *Conn) encodeTestFile(seq uint64, data []byte) ([]byte, error) {
	return c.encrypt(encodePayload(data), seq), nil
}

// discard keeps the linter honest about the os import when captureLog is the
// only user of stdio here; retained for future specs that need temp files.
var _ = os.DevNull