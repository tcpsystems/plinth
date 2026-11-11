package quota_test

import (
	"fmt"
	"time"

	"github.com/tcpsystems/plinth/quota"
)

func ExampleWindowStart() {
	t := time.Date(2026, 11, 10, 14, 32, 15, 0, time.UTC)
	w := quota.WindowStart(t, 1*time.Hour)
	fmt.Println(w.Format(time.RFC3339))

	w2 := quota.WindowStart(t, 0)
	fmt.Println(w2.Format(time.RFC3339))

	// Output: 2026-11-10T14:00:00Z
	// 2026-11-10T00:00:00Z
}
