package git

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// initRepo creates a bare-minimum git repo at dir with one empty commit.
func initRepo(t *testing.T, dir string) {
	t.Helper()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", args, out)
		}
	}
	run("git", "init", "-b", "main")
	run("git", "config", "user.email", "test@test.com")
	run("git", "config", "user.name", "Test")
	run("git", "commit", "--allow-empty", "-m", "init")
}

func TestDiscoverRepos_SingleRepo(t *testing.T) {
	dir := t.TempDir()
	initRepo(t, dir)

	repos, err := discoverRepos(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(repos) != 1 || repos[0] != dir {
		t.Fatalf("expected [%s], got %v", dir, repos)
	}
}

func TestDiscoverRepos_ParentFolder(t *testing.T) {
	parent := t.TempDir()

	for _, name := range []string{"repo-a", "repo-b"} {
		sub := filepath.Join(parent, name)
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		initRepo(t, sub)
	}
	// non-repo sibling should be ignored
	if err := os.Mkdir(filepath.Join(parent, "not-a-repo"), 0o755); err != nil {
		t.Fatal(err)
	}

	repos, err := discoverRepos(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(repos) != 2 {
		t.Fatalf("expected 2 repos, got %d: %v", len(repos), repos)
	}
}

func TestDiscoverRepos_NoRepos(t *testing.T) {
	dir := t.TempDir()
	repos, err := discoverRepos(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(repos) != 0 {
		t.Fatalf("expected no repos, got %v", repos)
	}
}

func TestIsGitRepo(t *testing.T) {
	dir := t.TempDir()
	if isGitRepo(dir) {
		t.Fatal("expected false for plain dir")
	}
	initRepo(t, dir)
	if !isGitRepo(dir) {
		t.Fatal("expected true after git init")
	}
}

func TestExpandPath_Tilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home dir")
	}
	got, err := expandPath("~/foo/bar")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, "foo", "bar")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExpandPath_Absolute(t *testing.T) {
	got, err := expandPath("/tmp/some/path")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/tmp/some/path" {
		t.Fatalf("unexpected result: %q", got)
	}
}

func TestExpandPath_TildeAlone(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home dir")
	}
	got, err := expandPath("~")
	if err != nil {
		t.Fatal(err)
	}
	if got != home {
		t.Fatalf("got %q, want %q", got, home)
	}
}

// TestWatcher_MissingPath checks that the watcher shuts down cleanly and
// emits an error event when the configured path does not exist.
func TestWatcher_MissingPath(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")

	w := &Watcher{
		name: "test",
		cfg: Config{
			Path:               missing,
			PollIntervalS:      300,
			AlertFilesChanged:  50,
			AlertBranchAgeDays: 14,
			MaxEvents:          100,
		},
		eventsCh: make(chan events.WatchEvent, 16),
		status:   watcher.StatusConnecting,
	}

	ctx := context.Background()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Drain until channel is closed; expect at least one error event.
	deadline := time.After(5 * time.Second)
	gotError := false
	for {
		select {
		case evt, ok := <-w.Events():
			if !ok {
				if !gotError {
					t.Fatal("channel closed without emitting an error event")
				}
				return
			}
			if evt.Type == events.EventError {
				gotError = true
			}
		case <-deadline:
			t.Fatal("timeout: watcher did not shut down")
		}
	}
}

// ── Poll integration helpers ──────────────────────────────────────────────────

// mustGit runs a git command in dir, fataling the test on error.
func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v in %s:\n%s", args, dir, out)
	}
}

// mustGitEnv is like mustGit but prepends extra environment variables.
func mustGitEnv(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v in %s:\n%s", args, dir, out)
	}
}

// drainEvents reads all buffered events from ch without blocking.
func drainEvents(ch <-chan events.WatchEvent) []events.WatchEvent {
	var out []events.WatchEvent
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

// setupPollEnv creates a three-repo topology:
//
//	origin - accepts pushes to its checked-out branch (denyCurrentBranch=ignore)
//	local  - cloned from origin; this is what the test watcher monitors
//	pusher - cloned from origin; simulates a second developer pushing changes
func setupPollEnv(t *testing.T) (origin, local, pusher string) {
	t.Helper()
	origin, local, pusher = t.TempDir(), t.TempDir(), t.TempDir()
	configure := func(dir string) {
		mustGit(t, dir, "config", "user.email", "test@test.com")
		mustGit(t, dir, "config", "user.name", "Test")
	}
	mustGit(t, origin, "init", "-b", "main")
	configure(origin)
	mustGit(t, origin, "config", "receive.denyCurrentBranch", "ignore")
	mustGit(t, origin, "commit", "--allow-empty", "-m", "init")
	mustGit(t, local, "clone", origin, ".")
	configure(local)
	mustGit(t, pusher, "clone", origin, ".")
	configure(pusher)
	return
}

// newSeededWatcher creates a Watcher with a seeded repoState for localPath.
func newSeededWatcher(t *testing.T, localPath string, cfg Config) (*Watcher, *repoState) {
	t.Helper()
	w := &Watcher{
		name:     "test",
		cfg:      cfg,
		eventsCh: make(chan events.WatchEvent, 32),
		status:   watcher.StatusConnecting,
	}
	rs := &repoState{path: localPath}
	w.fetchAndSeed(context.Background(), rs)
	return w, rs
}

// ── Poll integration tests ────────────────────────────────────────────────────

// TestPoll_NewRemoteCommits verifies that a new commit pushed to the current
// branch by another developer fires an EventMessage.
func TestPoll_NewRemoteCommits(t *testing.T) {
	_, local, pusher := setupPollEnv(t)
	w, rs := newSeededWatcher(t, local, Config{
		PollIntervalS:      300,
		AlertFilesChanged:  50,
		AlertBranchAgeDays: 14,
	})

	mustGit(t, pusher, "commit", "--allow-empty", "-m", "remote work")
	mustGit(t, pusher, "push", "origin", "main")

	w.poll(context.Background(), rs)

	evts := drainEvents(w.eventsCh)
	var found bool
	for _, e := range evts {
		if e.Type == events.EventMessage && strings.Contains(e.Summary, "remote advanced") {
			found = true
			count, ok := e.Detail["count"].(int)
			if !ok || count != 1 {
				t.Fatalf("expected count=1, got %v", e.Detail["count"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("no remote-advance event; got: %v", evts)
	}
}

// TestPoll_MainBranchAdvance verifies that new commits on origin/main fire an
// EventMessage suggesting a rebase when the local repo is on a feature branch.
func TestPoll_MainBranchAdvance(t *testing.T) {
	_, local, pusher := setupPollEnv(t)
	mustGit(t, local, "checkout", "-b", "feature")
	w, rs := newSeededWatcher(t, local, Config{
		PollIntervalS:      300,
		AlertFilesChanged:  50,
		AlertBranchAgeDays: 14,
	})
	if rs.mainBranch != "main" {
		t.Fatalf("expected mainBranch=main, got %q", rs.mainBranch)
	}

	mustGit(t, pusher, "commit", "--allow-empty", "-m", "hotfix on main")
	mustGit(t, pusher, "push", "origin", "main")

	w.poll(context.Background(), rs)

	evts := drainEvents(w.eventsCh)
	var found bool
	for _, e := range evts {
		if e.Type == events.EventMessage && strings.Contains(e.Summary, "consider rebasing") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no rebase-suggestion event; got: %v", evts)
	}
}

// TestPoll_FilesChangedThreshold verifies that an EventError is emitted when
// the number of files changed in the branch exceeds alert_files_changed.
func TestPoll_FilesChangedThreshold(t *testing.T) {
	_, local, _ := setupPollEnv(t)
	mustGit(t, local, "checkout", "-b", "big-feature")

	for i := 0; i < 6; i++ {
		p := filepath.Join(local, fmt.Sprintf("file%d.txt", i))
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustGit(t, local, "add", ".")
	mustGit(t, local, "commit", "-m", "add 6 files")

	// Threshold is 5 — 6 files should trigger the warning.
	w, rs := newSeededWatcher(t, local, Config{
		PollIntervalS:      300,
		AlertFilesChanged:  5,
		AlertBranchAgeDays: 14,
	})

	w.poll(context.Background(), rs)

	evts := drainEvents(w.eventsCh)
	var found bool
	for _, e := range evts {
		if e.Type == events.EventError && strings.Contains(e.Summary, "files changed") {
			found = true
			n, ok := e.Detail["files_changed"].(int)
			if !ok || n != 6 {
				t.Fatalf("expected files_changed=6, got %v", e.Detail["files_changed"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("no files-changed warning; got: %v", evts)
	}
}

// TestPoll_BranchAge verifies that an EventError is emitted when the branch
// is older than alert_branch_age_days.
func TestPoll_BranchAge(t *testing.T) {
	origin := t.TempDir()
	local := t.TempDir()

	// Seed origin with an init commit dated 30 days in the past so it becomes
	// the merge-base for the feature branch.
	mustGit(t, origin, "init", "-b", "main")
	mustGit(t, origin, "config", "user.email", "test@test.com")
	mustGit(t, origin, "config", "user.name", "Test")
	mustGit(t, origin, "config", "receive.denyCurrentBranch", "ignore")
	oldDate := time.Now().AddDate(0, 0, -30).UTC().Format(time.RFC3339)
	mustGitEnv(t, origin, []string{
		"GIT_COMMITTER_DATE=" + oldDate,
		"GIT_AUTHOR_DATE=" + oldDate,
	}, "commit", "--allow-empty", "-m", "old init")

	mustGit(t, local, "clone", origin, ".")
	mustGit(t, local, "config", "user.email", "test@test.com")
	mustGit(t, local, "config", "user.name", "Test")

	// Branch off the old init commit; the merge-base age will be ~30 days.
	mustGit(t, local, "checkout", "-b", "long-lived")
	mustGit(t, local, "commit", "--allow-empty", "-m", "work in progress")

	// Threshold is 14 days — 30-day-old branch should warn.
	w, rs := newSeededWatcher(t, local, Config{
		PollIntervalS:      300,
		AlertFilesChanged:  50,
		AlertBranchAgeDays: 14,
	})

	w.poll(context.Background(), rs)

	evts := drainEvents(w.eventsCh)
	var found bool
	for _, e := range evts {
		if e.Type == events.EventError && strings.Contains(e.Summary, "days old") {
			found = true
			age, ok := e.Detail["age_days"].(int)
			if !ok || age < 28 || age > 32 {
				t.Fatalf("expected age ~30 days, got %v", e.Detail["age_days"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("no branch-age warning; got: %v", evts)
	}
}

// TestWatcher_StartStop verifies that Start/Stop lifecycle works on a real repo.
func TestWatcher_StartStop(t *testing.T) {
	dir := t.TempDir()
	initRepo(t, dir)

	w := &Watcher{
		name: "test",
		cfg: Config{
			Path:               dir,
			PollIntervalS:      300, // long enough to never fire during test
			AlertFilesChanged:  50,
			AlertBranchAgeDays: 14,
			MaxEvents:          100,
		},
		eventsCh: make(chan events.WatchEvent, 32),
		status:   watcher.StatusConnecting,
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Expect a startup message.
	select {
	case evt, ok := <-w.Events():
		if !ok {
			t.Fatal("channel closed before startup event")
		}
		if evt.WatcherType != "git" {
			t.Fatalf("unexpected watcher type %q", evt.WatcherType)
		}
		if evt.WatcherName != "test" {
			t.Fatalf("unexpected watcher name %q", evt.WatcherName)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for startup event")
	}

	cancel()
	w.Stop()

	// Drain until channel closes.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-w.Events():
			if !ok {
				return // success
			}
		case <-deadline:
			t.Fatal("channel not closed after Stop")
		}
	}
}
