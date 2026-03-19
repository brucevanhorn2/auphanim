package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
