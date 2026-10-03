package ops

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func newRng() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestNextInsertsWhenNoUsers(t *testing.T) {
	rng := newRng()
	for range 100 {
		if k := Next(rng, 0, 10); k != Insert {
			t.Fatalf("live=0 phải Insert, được %v", k)
		}
	}
}

func TestNextNeverInsertsAtCap(t *testing.T) {
	rng := newRng()
	for range 2000 {
		if k := Next(rng, 10, 10); k == Insert {
			t.Fatal("đạt trần mà vẫn Insert")
		}
	}
}

func TestNextCoversEveryKind(t *testing.T) {
	rng := newRng()
	seen := map[Kind]int{}
	for range 5000 {
		seen[Next(rng, 5, 100)]++
	}
	for _, k := range []Kind{UpdateCity, UpdateGender, ClearCity, Insert, Delete} {
		if seen[k] == 0 {
			t.Errorf("không bao giờ chọn %v", k)
		}
	}
	if seen[UpdateCity] < seen[Delete] {
		t.Errorf("update_city (%d) phải nhiều hơn delete (%d)", seen[UpdateCity], seen[Delete])
	}
}

func TestNewCityAndGenderDiffer(t *testing.T) {
	rng := newRng()
	for range 200 {
		if NewCity("HN", rng) == "HN" {
			t.Fatal("NewCity trả city cũ")
		}
		if NewGender("F", rng) == "F" {
			t.Fatal("NewGender trả giới tính cũ")
		}
	}
	if c := NewCity("", rng); c == "" {
		t.Fatal("NewCity rỗng")
	}
}

func TestNewGenderOrNilHasBoth(t *testing.T) {
	rng := newRng()
	nils, vals := 0, 0
	for range 400 {
		if NewGenderOrNil(rng) == nil {
			nils++
		} else {
			vals++
		}
	}
	if nils == 0 || vals == 0 {
		t.Fatalf("cần cả nil và giá trị: nil=%d val=%d", nils, vals)
	}
}

func TestNewUserIDIsLive(t *testing.T) {
	rng := newRng()
	id := NewUserID(rng)
	if !strings.HasPrefix(id, "L") || len(id) != 10 || !IsLive(id) {
		t.Fatalf("mã user sai: %q", id)
	}
	for _, seed := range []string{"U1001", "G0000001", ""} {
		if IsLive(seed) {
			t.Errorf("%q không phải live", seed)
		}
	}
}

func TestNewBirthDateInRange(t *testing.T) {
	rng := newRng()
	lo, hi := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC)
	for range 500 {
		if d := NewBirthDate(rng); d.Before(lo) || !d.Before(hi) {
			t.Fatalf("ngày sinh ngoài khoảng: %v", d)
		}
	}
}
