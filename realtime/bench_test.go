package realtime

import (
	"encoding/json"
	"testing"
)

// BenchmarkPublish measures appending an event to the hub ring with no
// subscribers, the cost every producer pays per event.
func BenchmarkPublish(b *testing.B) {
	h := NewHub(HubOptions{})
	data := json.RawMessage(`{"run":"018f","state":"started"}`)
	topics := []string{"acct/1", "acct/2", "acct/3"}
	b.ResetTimer()
	i := 0
	for b.Loop() {
		i++
		if _, err := h.Publish(Event{Topic: topics[i%len(topics)], Kind: "run.started", Data: data}); err != nil {
			b.Fatal(err)
		}
	}
}
