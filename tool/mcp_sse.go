package tool

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// mcpMaxMessage bounds one message from an MCP server (a JSON body or one
// event): a server must not make the worker hold an unbounded answer.
const mcpMaxMessage = 16 << 20

// sseEvent is one event of a text/event-stream.
type sseEvent struct {
	event string // "" = "message"
	data  string
}

// sseReader reads the events of a text/event-stream as the HTML standard
// defines them. It keeps the last event ID and retry delay the server gave,
// which resuming a stream needs.
type sseReader struct {
	r      *bufio.Reader
	lastID string
	retry  time.Duration
}

func newSSEReader(r io.Reader) *sseReader {
	return &sseReader{r: bufio.NewReader(r)}
}

var errSSETooLarge = errors.New("mcp: event larger than the limit")

// next returns the next event with data. Comments and events without data
// (a priming event carrying only an ID) are consumed, not returned.
func (s *sseReader) next() (sseEvent, error) {
	var ev sseEvent
	var data strings.Builder
	hasData := false
	for {
		line, err := s.readLine()
		if err != nil {
			// An event cut by the end of the stream is dropped, as the
			// standard says.
			return sseEvent{}, err
		}
		if line == "" {
			if !hasData || data.Len() == 0 {
				ev, hasData = sseEvent{}, false
				data.Reset()
				continue
			}
			ev.data = data.String()
			return ev, nil
		}
		if line[0] == ':' {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.event = value
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
			if data.Len() > mcpMaxMessage {
				return sseEvent{}, errSSETooLarge
			}
		case "id":
			if !strings.ContainsRune(value, 0) {
				s.lastID = value
			}
		case "retry":
			if ms, err := strconv.Atoi(value); err == nil && ms >= 0 {
				s.retry = time.Duration(ms) * time.Millisecond
			}
		}
	}
}

// readLine reads one line without its end (\n or \r\n), up to the limit.
func (s *sseReader) readLine() (string, error) {
	var line []byte
	for {
		chunk, isPrefix, err := s.r.ReadLine()
		if err != nil {
			return "", err
		}
		line = append(line, chunk...)
		if len(line) > mcpMaxMessage {
			return "", errSSETooLarge
		}
		if !isPrefix {
			return string(line), nil
		}
	}
}
