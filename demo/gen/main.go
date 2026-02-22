// gen is a traffic generator for the auphanim demo.
// It continuously inserts, updates, and deletes rows in PostgreSQL,
// publishes corresponding events to Kafka, writes receipt files to disk,
// and sets/expires/deletes keys in Redis — so you can watch all four
// watcher types light up in the TUI at the same time.
//
// Run from the project root:
//
//	go run ./demo/gen
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	kafkago "github.com/segmentio/kafka-go"

	goredis "github.com/redis/go-redis/v9"
)

const (
	pgDSN       = "postgres://demo:demo@localhost:5432/demo"
	kafkaBroker = "localhost:9092"
	kafkaTopic  = "demo.events"
	uploadDir   = "/tmp/auphanim-demo"
	redisAddr   = "localhost:6379"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── PostgreSQL ────────────────────────────────────────────────────────────

	conn := mustConnectPG(ctx)
	defer conn.Close(context.Background())

	if err := setupSchema(ctx, conn); err != nil {
		log.Fatalf("schema setup: %v", err)
	}

	// ── Kafka ─────────────────────────────────────────────────────────────────

	kw := &kafkago.Writer{
		Addr:                   kafkago.TCP(kafkaBroker),
		Topic:                  kafkaTopic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafkago.LeastBytes{},
	}
	defer kw.Close()

	// ── Filesystem dir ────────────────────────────────────────────────────────

	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", uploadDir, err)
	}

	// ── Redis ─────────────────────────────────────────────────────────────────

	rc := mustConnectRedis(ctx)
	defer rc.Close()

	// ── Load existing IDs (so we can resume after a restart) ─────────────────

	users := queryIDs(ctx, conn, "SELECT id FROM users ORDER BY id")
	orders := queryIDs(ctx, conn, "SELECT id FROM orders ORDER BY id")
	var redisKeys []string

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  auphanim demo traffic generator")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  postgres  → %s\n", pgDSN)
	fmt.Printf("  kafka     → %s  topic=%s\n", kafkaBroker, kafkaTopic)
	fmt.Printf("  files     → %s/*.json\n", uploadDir)
	fmt.Printf("  redis     → %s\n", redisAddr)
	fmt.Printf("  existing    %d users, %d orders\n", len(users), len(orders))
	fmt.Println()
	fmt.Println("  In another terminal run:")
	fmt.Println("    ./auphanim -c demo/auphanim.json")
	fmt.Println()
	fmt.Println("  Ctrl-C to stop.")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("stopped.")
			return
		case <-time.After(time.Duration(800+rand.Intn(1800)) * time.Millisecond):
			users, orders, redisKeys = tick(ctx, conn, kw, rc, users, orders, redisKeys)
		}
	}
}

// ── Schema ────────────────────────────────────────────────────────────────────

func setupSchema(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id         SERIAL PRIMARY KEY,
			name       TEXT        NOT NULL,
			email      TEXT        NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS orders (
			id         SERIAL PRIMARY KEY,
			user_id    INT         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			product    TEXT        NOT NULL,
			total      NUMERIC(10,2) NOT NULL,
			status     TEXT        NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	return err
}

// ── Tick ──────────────────────────────────────────────────────────────────────

var (
	firstNames = []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack"}
	products   = []string{"Widget", "Gadget", "Doohickey", "Thingamajig", "Whatsit", "Gizmo", "Contraption"}
	statuses   = []string{"processing", "shipped", "delivered", "cancelled"}
)

type weightedAction struct {
	name   string
	weight int
	avail  func() bool
}

func tick(
	ctx context.Context,
	conn *pgx.Conn,
	kw *kafkago.Writer,
	rc *goredis.Client,
	users, orders []int,
	redisKeys []string,
) ([]int, []int, []string) {
	actions := []weightedAction{
		{"insert_user", 3, func() bool { return true }},
		{"insert_order", 5, func() bool { return len(users) > 0 }},
		{"update_order", 6, func() bool { return len(orders) > 0 }},
		{"delete_user", 1, func() bool { return len(users) > 3 }},
		{"redis_set", 4, func() bool { return true }},
		{"redis_setex", 3, func() bool { return true }},
		{"redis_del", 2, func() bool { return len(redisKeys) > 2 }},
	}

	chosen := pickWeighted(actions)
	if chosen == "" {
		return users, orders, redisKeys
	}

	switch chosen {

	case "insert_user":
		name := firstNames[rand.Intn(len(firstNames))]
		email := fmt.Sprintf("%s%d@example.com", name, rand.Intn(9000)+1000)
		var id int
		err := conn.QueryRow(ctx,
			"INSERT INTO users(name,email) VALUES($1,$2) RETURNING id", name, email,
		).Scan(&id)
		if err != nil {
			break
		}
		users = append(users, id)
		emit(ctx, kw, "user.created", map[string]any{"userId": id, "name": name, "email": email})
		logf("INSERT", "users", "id=%-4d name=%q email=%s", id, name, email)

	case "insert_order":
		uid := users[rand.Intn(len(users))]
		product := products[rand.Intn(len(products))]
		price := float64(rand.Intn(9900)+100) / 100.0
		var id int
		err := conn.QueryRow(ctx,
			"INSERT INTO orders(user_id,product,total) VALUES($1,$2,$3) RETURNING id",
			uid, product, price,
		).Scan(&id)
		if err != nil {
			break
		}
		orders = append(orders, id)

		payload := map[string]any{
			"orderId": id, "userId": uid, "product": product, "total": price, "status": "pending",
		}
		emit(ctx, kw, "order.created", payload)

		// Write a receipt JSON file — triggers the filesystem watcher.
		data, _ := json.MarshalIndent(payload, "", "  ")
		_ = os.WriteFile(filepath.Join(uploadDir, fmt.Sprintf("order-%d.json", id)), data, 0o644)

		logf("INSERT", "orders", "id=%-4d user_id=%-4d product=%q total=%.2f", id, uid, product, price)

	case "update_order":
		oid := orders[rand.Intn(len(orders))]
		status := statuses[rand.Intn(len(statuses))]
		_, err := conn.Exec(ctx, "UPDATE orders SET status=$1 WHERE id=$2", status, oid)
		if err != nil {
			break
		}
		emit(ctx, kw, "order.updated", map[string]any{"orderId": oid, "status": status})
		logf("UPDATE", "orders", "id=%-4d status=%q", oid, status)

	case "delete_user":
		idx := rand.Intn(len(users))
		uid := users[idx]
		_, err := conn.Exec(ctx, "DELETE FROM users WHERE id=$1", uid)
		if err != nil {
			break
		}
		users = append(users[:idx], users[idx+1:]...)
		// Cascade deletes their orders; reload the order IDs from DB.
		orders = queryIDs(ctx, conn, "SELECT id FROM orders ORDER BY id")
		emit(ctx, kw, "user.deleted", map[string]any{"userId": uid})
		logf("DELETE", "users ", "id=%-4d (orders cascade-deleted)", uid)

	case "redis_set":
		// Permanent key — stays until explicitly deleted.
		key := fmt.Sprintf("session:%d", rand.Intn(9000)+1000)
		val := fmt.Sprintf(`{"userId":%d,"role":"user"}`, rand.Intn(100))
		rc.Set(ctx, key, val, 0)
		redisKeys = append(redisKeys, key)
		logf("SET", "redis ", "key=%s", key)

	case "redis_setex":
		// Expiring key — simulates a short-lived token or rate-limit counter.
		key := fmt.Sprintf("token:%d", rand.Intn(9000)+1000)
		val := fmt.Sprintf("tok_%d", rand.Intn(100000))
		ttl := time.Duration(10+rand.Intn(20)) * time.Second
		rc.Set(ctx, key, val, ttl)
		redisKeys = append(redisKeys, key)
		logf("SETEX", "redis ", "key=%s ttl=%s", key, ttl)

	case "redis_del":
		idx := rand.Intn(len(redisKeys))
		key := redisKeys[idx]
		rc.Del(ctx, key)
		redisKeys = append(redisKeys[:idx], redisKeys[idx+1:]...)
		logf("DEL", "redis ", "key=%s", key)
	}

	return users, orders, redisKeys
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func pickWeighted(actions []weightedAction) string {
	total := 0
	for _, a := range actions {
		if a.avail() {
			total += a.weight
		}
	}
	if total == 0 {
		return ""
	}
	r := rand.Intn(total)
	for _, a := range actions {
		if !a.avail() {
			continue
		}
		r -= a.weight
		if r < 0 {
			return a.name
		}
	}
	return ""
}

func emit(ctx context.Context, w *kafkago.Writer, eventType string, data map[string]any) {
	body, _ := json.Marshal(map[string]any{
		"event": eventType,
		"ts":    time.Now().UnixMilli(),
		"data":  data,
	})
	_ = w.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(eventType),
		Value: body,
	})
}

func queryIDs(ctx context.Context, conn *pgx.Conn, q string) []int {
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func mustConnectPG(ctx context.Context) *pgx.Conn {
	var (
		conn *pgx.Conn
		err  error
	)
	for attempt := 1; attempt <= 20; attempt++ {
		conn, err = pgx.Connect(ctx, pgDSN)
		if err == nil {
			return conn
		}
		fmt.Printf("  waiting for PostgreSQL (attempt %d/20): %v\n", attempt, err)
		select {
		case <-ctx.Done():
			log.Fatalf("context cancelled while waiting for PostgreSQL")
		case <-time.After(2 * time.Second):
		}
	}
	log.Fatalf("could not connect to PostgreSQL after 20 attempts: %v", err)
	return nil
}

func mustConnectRedis(ctx context.Context) *goredis.Client {
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	for attempt := 1; attempt <= 20; attempt++ {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		} else {
			fmt.Printf("  waiting for Redis (attempt %d/20): %v\n", attempt, err)
		}
		select {
		case <-ctx.Done():
			log.Fatalf("context cancelled while waiting for Redis")
		case <-time.After(2 * time.Second):
		}
	}
	log.Fatalf("could not connect to Redis after 20 attempts")
	return nil
}

func logf(op, table, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("[%s]  %-6s  %-7s  %s\n", time.Now().Format("15:04:05"), op, table, msg)
}
