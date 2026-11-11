// Command probe prints buildinfo.Get as JSON. Tests run it with and without
// VCS stamps to exercise both build paths.
package main

import (
	"encoding/json"
	"os"

	"github.com/tcpsystems/plinth/buildinfo"
)

func main() {
	_ = json.NewEncoder(os.Stdout).Encode(buildinfo.Get())
}
