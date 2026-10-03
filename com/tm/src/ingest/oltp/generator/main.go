// generator: mỗi 1/RATE_PER_SEC giây thực hiện một thao tác ngẫu nhiên lên src.user_profile của Postgres OLTP
// (đổi city, đổi giới tính, xoá city, thêm user, xoá user) để có luồng CDC liên tục cho Debezium/Flink.
// Chỉ đụng user do chính nó sinh (mã `L…`), không đụng user seed (U1001…) nên golden không bị ảnh hưởng.
//
// Biến môi trường: PG_DSN, RATE_PER_SEC (mặc định 1), MAX_USERS (trần số user sinh ra, mặc định 10000).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"com.tm/vision/com/tm/src/ingest/oltp/generator/internal/ops"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("dừng do lỗi", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn := env("PG_DSN", "postgres://vision:vision@localhost:5433/oltp")
	rate := envInt("RATE_PER_SEC", 1)
	maxUsers := envInt("MAX_USERS", 10000)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("kết nối Postgres: %w", err)
	}
	defer pool.Close()

	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()

	log.Info("bắt đầu", "ratePerSec", rate, "maxUsers", maxUsers)
	var done int64
	for {
		select {
		case <-ctx.Done():
			log.Info("dừng", "operations", done)
			return nil
		case <-tick.C:
			kind, userID, err := step(ctx, pool, rng, maxUsers)
			if err != nil {
				// Postgres chưa sẵn sàng hoặc bảng chưa có: báo rồi thử lại ở nhịp sau, không thoát
				log.Warn("thao tác lỗi", "err", err)
				continue
			}
			done++
			log.Info("thao tác", "op", kind.String(), "user", userID, "total", done)
		}
	}
}

// step thực hiện đúng một thao tác lên src.user_profile và trả về loại thao tác + user bị tác động.
func step(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, maxUsers int) (ops.Kind, string, error) {
	var live int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM src.user_profile WHERE user_id LIKE 'L%'`).Scan(&live); err != nil {
		return 0, "", err
	}
	kind := ops.Next(rng, live, maxUsers)
	if kind == ops.Insert {
		id := ops.NewUserID(rng)
		_, err := pool.Exec(ctx, `
			INSERT INTO src.user_profile (user_id, city_code, birth_date, gender)
			VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING`,
			id, ops.Cities[rng.IntN(len(ops.Cities))], ops.NewBirthDate(rng), ops.NewGenderOrNil(rng))
		return kind, id, err
	}

	var id string
	var city, gender *string
	err := pool.QueryRow(ctx, `
		SELECT user_id, city_code, gender FROM src.user_profile
		WHERE user_id LIKE 'L%' ORDER BY random() LIMIT 1`).Scan(&id, &city, &gender)
	if errors.Is(err, pgx.ErrNoRows) {
		return kind, "", nil
	}
	if err != nil {
		return kind, "", err
	}
	switch kind {
	case ops.UpdateCity:
		_, err = pool.Exec(ctx, `UPDATE src.user_profile SET city_code = $1, updated_at = now() WHERE user_id = $2`,
			ops.NewCity(deref(city), rng), id)
	case ops.UpdateGender:
		_, err = pool.Exec(ctx, `UPDATE src.user_profile SET gender = $1, updated_at = now() WHERE user_id = $2`,
			ops.NewGender(deref(gender), rng), id)
	case ops.ClearCity:
		_, err = pool.Exec(ctx, `UPDATE src.user_profile SET city_code = NULL, updated_at = now() WHERE user_id = $1`, id)
	case ops.Delete:
		_, err = pool.Exec(ctx, `DELETE FROM src.user_profile WHERE user_id = $1`, id)
	default:
		return kind, id, fmt.Errorf("thao tác chưa hỗ trợ: %v", kind)
	}
	return kind, id, err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
