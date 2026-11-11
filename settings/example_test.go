package settings_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tcpsystems/plinth/settings"
)

// mockSource implements settings.Source for demonstration.
type mockSource struct{}

func (m *mockSource) Load(ctx context.Context, key string) (settings.Setting, error) {
	if key == "theme" {
		return settings.Setting{
			Key:     "theme",
			Value:   json.RawMessage(`"dark"`),
			Version: 1,
		}, nil
	}
	return settings.Setting{}, errors.New("not found")
}

func ExampleNew() {
	src := &mockSource{}
	loader := settings.New(src, 30*time.Second, nil)

	ctx := context.Background()
	s, err := loader.Get(ctx, "theme")
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Key)
	fmt.Println(string(s.Value))

	_, err = loader.Get(ctx, "missing")
	fmt.Println(err != nil)

	// Output: theme
	// "dark"
	// true
}
