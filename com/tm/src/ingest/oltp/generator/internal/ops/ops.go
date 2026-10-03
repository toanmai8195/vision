// Package ops chọn thao tác ngẫu nhiên lên src.user_profile (thuần logic, không đụng DB nên test được).
package ops

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Kind là loại thao tác của một nhịp.
type Kind int

const (
	UpdateCity   Kind = iota // đổi city sang city khác
	UpdateGender             // đổi giới tính
	ClearCity                // city -> NULL (mất city, REMOVED với user_city)
	Insert                   // user mới
	Delete                   // xoá dòng
)

func (k Kind) String() string {
	switch k {
	case UpdateCity:
		return "update_city"
	case UpdateGender:
		return "update_gender"
	case ClearCity:
		return "clear_city"
	case Insert:
		return "insert"
	case Delete:
		return "delete"
	}
	return "unknown"
}

var (
	Cities  = []string{"HCM", "HN", "DN", "CT"}
	Genders = []string{"M", "F", "O"}
)

// weights: tỉ lệ tương đối của từng thao tác khi đã có user để sửa.
var weights = []struct {
	kind Kind
	w    int
}{
	{UpdateCity, 60}, {UpdateGender, 10}, {ClearCity, 5}, {Insert, 20}, {Delete, 5},
}

// Next chọn thao tác cho nhịp này. live = số user do generator sinh ra; max = trần số user.
// Chưa có user nào thì chỉ có thể Insert; đạt trần thì không Insert thêm.
func Next(rng *rand.Rand, live, max int) Kind {
	if live == 0 {
		return Insert
	}
	total := 0
	for _, e := range weights {
		if e.kind == Insert && live >= max {
			continue
		}
		total += e.w
	}
	n := rng.IntN(total)
	for _, e := range weights {
		if e.kind == Insert && live >= max {
			continue
		}
		if n < e.w {
			return e.kind
		}
		n -= e.w
	}
	panic("không tới được")
}

// NewCity trả về city khác `cur` (cur rỗng/NULL thì chọn bất kỳ).
func NewCity(cur string, rng *rand.Rand) string {
	for {
		c := Cities[rng.IntN(len(Cities))]
		if c != cur {
			return c
		}
	}
}

// NewGender trả về M/F/O khác `cur`.
func NewGender(cur string, rng *rand.Rand) string {
	for {
		g := Genders[rng.IntN(len(Genders))]
		if g != cur {
			return g
		}
	}
}

// NewGenderOrNil: ~25% chưa khai báo (nil), còn lại M/F/O.
func NewGenderOrNil(rng *rand.Rand) *string {
	if rng.IntN(4) == 0 {
		return nil
	}
	g := Genders[rng.IntN(len(Genders))]
	return &g
}

// NewUserID sinh mã user có tiền tố L (không đụng U1001.. của seed và G.. của generate.py).
func NewUserID(rng *rand.Rand) string {
	return fmt.Sprintf("L%09d", rng.Int64N(1_000_000_000))
}

// IsLive cho biết mã user có do generator sinh ra không.
func IsLive(userID string) bool { return len(userID) > 0 && userID[0] == 'L' }

// NewBirthDate chọn ngày sinh trong 1960-01-01 .. 2008-12-31.
func NewBirthDate(rng *rand.Rand) time.Time {
	start := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	return start.AddDate(0, 0, rng.IntN(365*49))
}
