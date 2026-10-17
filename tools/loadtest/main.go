// Command loadtest drives concurrent traffic for one tenant across one or
// more base URLs and verifies that the deployment admitted exactly the
// configured burst. It prints the per-instance split (X-Instance-ID) and
// latency percentiles, and exits non-zero on any correctness failure.
//
//	go run ./tools/loadtest -urls http://localhost:8080 -token "$ADMIN_TOKEN" -fresh-conns
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	allowed  bool
	instance string
	latency  time.Duration
	err      error
}

func main() {
	var (
		urlsFlag   = flag.String("urls", "http://localhost:8080", "comma-separated base URLs")
		token      = flag.String("token", os.Getenv("ADMIN_TOKEN"), "admin token (defaults to $ADMIN_TOKEN)")
		algorithm  = flag.String("algorithm", "token_bucket", "token_bucket or sliding_window")
		burst      = flag.Int("burst", 100, "requests that may be admitted")
		requests   = flag.Int("n", 5000, "total requests")
		concurrent = flag.Int("c", 200, "concurrent workers")
		freshConns = flag.Bool("fresh-conns", false, "new connection per request so a load balancer spreads traffic")
		minInst    = flag.Int("min-instances", 1, "fail if fewer distinct instances served traffic")
	)
	flag.Parse()
	if *token == "" {
		fail("admin token required: -token or $ADMIN_TOKEN")
	}
	urls := strings.Split(*urlsFlag, ",")
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *concurrent, DisableKeepAlives: *freshConns},
	}

	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	tenant := "load-" + hex.EncodeToString(suffix)

	// A refill of 1 per hour means only the burst can be admitted during the run.
	rule := map[string]any{"tenant": tenant, "namespace": "loadtest", "algorithm": *algorithm, "period": "1h"}
	if *algorithm == "token_bucket" {
		rule["limit"], rule["burst"] = 1, *burst
	} else {
		rule["limit"] = *burst
	}
	ruleID := createRule(client, urls[0], *token, rule)
	defer deleteRule(client, urls[0], *token, ruleID)
	time.Sleep(500 * time.Millisecond) // let every instance's cache receive the change event

	fmt.Printf("tenant=%s namespace=loadtest rule: %s burst=%d refill=1/h\n", tenant, *algorithm, *burst)
	fmt.Printf("requests: %d  concurrency: %d  fresh-conns: %v  targets: %v\n", *requests, *concurrent, *freshConns, urls)

	body, _ := json.Marshal(map[string]any{"tenant": tenant, "namespace": "loadtest"})
	jobs := make(chan int)
	results := make(chan result, *requests)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrent; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results <- check(client, urls[i%len(urls)], body)
			}
		}()
	}
	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	var (
		allowed, denied, errs int
		latencies             []time.Duration
		perInstance           = map[string][2]int{}
		firstErr              error
	)
	for r := range results {
		if r.err != nil {
			errs++
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		latencies = append(latencies, r.latency)
		c := perInstance[r.instance]
		c[0]++
		if r.allowed {
			allowed++
			c[1]++
		} else {
			denied++
		}
		perInstance[r.instance] = c
	}

	fmt.Printf("duration: %s  throughput: %.0f req/s\n", elapsed.Round(time.Millisecond), float64(*requests)/elapsed.Seconds())
	fmt.Printf("allowed: %d  denied: %d  errors: %d  expected allowed: %d\n", allowed, denied, errs, *burst)
	fmt.Println("by instance:")
	names := make([]string, 0, len(perInstance))
	for n := range perInstance {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Printf("  %-40s served %5d  allowed %4d\n", n, perInstance[n][0], perInstance[n][1])
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p := func(q float64) time.Duration {
			return latencies[int(q*float64(len(latencies)-1))].Round(10 * time.Microsecond)
		}
		fmt.Printf("latency p50 %s  p95 %s  p99 %s  max %s\n", p(.50), p(.95), p(.99), latencies[len(latencies)-1].Round(10*time.Microsecond))
	}

	ok := true
	if errs > 0 {
		fmt.Printf("FAIL: %d request errors (first: %v)\n", errs, firstErr)
		ok = false
	}
	if allowed != *burst {
		fmt.Printf("FAIL: allowed %d, expected exactly %d\n", allowed, *burst)
		ok = false
	}
	if len(perInstance) < *minInst {
		fmt.Printf("FAIL: traffic reached %d instance(s), expected at least %d (try -fresh-conns behind a load balancer)\n", len(perInstance), *minInst)
		ok = false
	}
	if !ok {
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func check(client *http.Client, base string, body []byte) result {
	start := time.Now()
	resp, err := client.Post(base+"/v1/check", "application/json", bytes.NewReader(body))
	if err != nil {
		return result{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return result{err: fmt.Errorf("status %d: %s", resp.StatusCode, b)}
	}
	var r struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return result{err: err}
	}
	return result{allowed: r.Allowed, instance: resp.Header.Get("X-Instance-ID"), latency: time.Since(start)}
}

func createRule(client *http.Client, base, token string, rule map[string]any) string {
	data, _ := json.Marshal(rule)
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/rules", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		fail("create rule: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		fail("create rule: status %d: %s", resp.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(b, &created)
	return created.ID
}

func deleteRule(client *http.Client, base, token, id string) {
	req, _ := http.NewRequest(http.MethodDelete, base+"/v1/rules/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
