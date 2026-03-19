// Package git watches one or more local git repositories for activity.
//
// Point "path" at a single git repo OR at a parent directory that contains
// multiple repos (one level deep). The watcher polls on poll_interval_s
// (default 60 s) by running a `git fetch` and then inspecting branch state.
//
// Events emitted:
//   - Startup: repos discovered and watched
//   - New commits pushed to the current branch by someone else
//   - New commits landed on main/master that you may need to rebase onto
//   - Warning when files-changed count in the branch exceeds alert_files_changed
//   - Warning when the branch has lived longer than alert_branch_age_days
package git

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

const (
	defaultPollInterval    = 60
	defaultAlertFiles      = 50
	defaultAlertBranchDays = 14
)

func init() {
	watcher.Register("git", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		cfg := Config{
			PollIntervalS:      defaultPollInterval,
			AlertFilesChanged:  defaultAlertFiles,
			AlertBranchAgeDays: defaultAlertBranchDays,
			MaxEvents:          100,
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &cfg); err != nil {
				return nil, err
			}
		}
		if cfg.PollIntervalS <= 0 {
			cfg.PollIntervalS = defaultPollInterval
		}
		if cfg.AlertFilesChanged <= 0 {
			cfg.AlertFilesChanged = defaultAlertFiles
		}
		if cfg.AlertBranchAgeDays <= 0 {
			cfg.AlertBranchAgeDays = defaultAlertBranchDays
		}
		if cfg.Path == "" {
			return nil, fmt.Errorf("git watcher %q: path is required", name)
		}
		return &Watcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

// Config holds git-watcher-specific configuration.
type Config struct {
	// Path to a git repo OR a directory whose immediate subdirectories are repos.
	Path string `json:"path"`

	// How often to poll (run git fetch + inspect). Default: 60 s.
	PollIntervalS int `json:"poll_interval_s"`

	// Emit a warning when a branch has more than this many files changed vs
	// main/master. 0 disables the check. Default: 50.
	AlertFilesChanged int `json:"alert_files_changed"`

	// Emit a warning when a branch is older than this many days. 0 disables
	// the check. Default: 14.
	AlertBranchAgeDays int `json:"alert_branch_age_days"`

	MaxEvents int `json:"max_events"`
}

// repoState tracks per-repo inter-poll state.
type repoState struct {
	path            string
	currentBranch   string
	lastRemoteHead  string // last seen origin/<currentBranch> SHA
	lastMainHead    string // last seen origin/<main|master> SHA
	filesWarnIssued bool   // true once we've fired the files-changed warning
	ageWarnIssued   bool   // true once we've fired the branch-age warning
}

// Watcher is the git watcher implementation.
type Watcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
	repos    []*repoState
}

func (w *Watcher) Name() string                     { return w.name }
func (w *Watcher) Type() string                     { return "git" }
func (w *Watcher) Events() <-chan events.WatchEvent { return w.eventsCh }

func (w *Watcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *Watcher) setStatus(s watcher.Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}

func (w *Watcher) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	go w.run(ctx)
	return nil
}

func (w *Watcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Watcher) emit(t events.EventType, summary string, detail map[string]any) {
	select {
	case w.eventsCh <- events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "git",
		Type:        t,
		Summary:     summary,
		Detail:      detail,
	}:
	default:
	}
}

func (w *Watcher) run(ctx context.Context) {
	defer close(w.eventsCh)
	defer w.setStatus(watcher.StatusStopped)

	// Resolve and discover repos.
	root, err := expandPath(w.cfg.Path)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.emit(events.EventError, fmt.Sprintf("cannot expand path %q: %v", w.cfg.Path, err), nil)
		return
	}

	paths, err := discoverRepos(root)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.emit(events.EventError, fmt.Sprintf("cannot discover repos in %q: %v", root, err), nil)
		return
	}
	if len(paths) == 0 {
		w.setStatus(watcher.StatusError)
		w.emit(events.EventError, fmt.Sprintf("no git repos found in %q", root), nil)
		return
	}

	w.repos = make([]*repoState, len(paths))
	for i, p := range paths {
		w.repos[i] = &repoState{path: p}
	}

	// Announce what we found.
	w.setStatus(watcher.StatusConnected)
	if len(paths) == 1 {
		w.emit(events.EventMessage,
			fmt.Sprintf("watching repo: %s", filepath.Base(paths[0])),
			map[string]any{"path": paths[0]})
	} else {
		names := make([]string, len(paths))
		for i, p := range paths {
			names[i] = filepath.Base(p)
		}
		w.emit(events.EventMessage,
			fmt.Sprintf("watching %d repos under %s", len(paths), filepath.Base(root)),
			map[string]any{"repos": names, "root": root})
	}

	// Seed state without emitting events (establishes baseline).
	for _, rs := range w.repos {
		w.fetchAndSeed(ctx, rs)
	}

	ticker := time.NewTicker(time.Duration(w.cfg.PollIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, rs := range w.repos {
				w.poll(ctx, rs)
			}
		}
	}
}

// fetchAndSeed runs git fetch and records the current remote state as the
// baseline so the first real poll only reports changes from that point on.
func (w *Watcher) fetchAndSeed(ctx context.Context, rs *repoState) {
	gitFetch(ctx, rs.path)

	branch, err := gitOut(ctx, rs.path, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return
	}
	rs.currentBranch = branch
	rs.lastRemoteHead, _ = gitOut(ctx, rs.path, "rev-parse", "origin/"+branch)
	if mb := mainBranch(ctx, rs.path); mb != "" {
		rs.lastMainHead, _ = gitOut(ctx, rs.path, "rev-parse", "origin/"+mb)
	}
}

// poll fetches and checks for interesting changes, emitting events as needed.
func (w *Watcher) poll(ctx context.Context, rs *repoState) {
	gitFetch(ctx, rs.path)

	repoLabel := filepath.Base(rs.path)

	// ── Current branch ────────────────────────────────────────────────────────
	branch, err := gitOut(ctx, rs.path, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return
	}

	// Branch switch: reset remote tracking for the new branch.
	if rs.currentBranch != "" && branch != rs.currentBranch {
		w.emit(events.EventMessage,
			fmt.Sprintf("[%s] switched branch: %s → %s", repoLabel, rs.currentBranch, branch),
			map[string]any{"repo": rs.path, "old": rs.currentBranch, "new": branch})
		rs.currentBranch = branch
		rs.lastRemoteHead, _ = gitOut(ctx, rs.path, "rev-parse", "origin/"+branch)
		rs.filesWarnIssued = false
		rs.ageWarnIssued = false
	}
	rs.currentBranch = branch

	// ── Remote pushes to the current branch ───────────────────────────────────
	remoteHead, _ := gitOut(ctx, rs.path, "rev-parse", "origin/"+branch)
	if remoteHead != "" && rs.lastRemoteHead != "" && remoteHead != rs.lastRemoteHead {
		commits, _ := gitLines(ctx, rs.path, "log",
			rs.lastRemoteHead+".."+remoteHead, "--oneline", "--no-decorate")
		w.emit(events.EventMessage,
			fmt.Sprintf("[%s] %d new commit(s) pushed to %s by someone else",
				repoLabel, len(commits), branch),
			map[string]any{
				"repo":    rs.path,
				"branch":  branch,
				"commits": commits,
				"count":   len(commits),
			})
	}
	if remoteHead != "" {
		rs.lastRemoteHead = remoteHead
	}

	// ── Activity on main/master ───────────────────────────────────────────────
	mb := mainBranch(ctx, rs.path)
	if mb != "" && branch != mb {
		mainHead, _ := gitOut(ctx, rs.path, "rev-parse", "origin/"+mb)
		if mainHead != "" && rs.lastMainHead != "" && mainHead != rs.lastMainHead {
			commits, _ := gitLines(ctx, rs.path, "log",
				rs.lastMainHead+".."+mainHead, "--oneline", "--no-decorate")
			w.emit(events.EventMessage,
				fmt.Sprintf("[%s] %d new commit(s) landed on %s — consider rebasing",
					repoLabel, len(commits), mb),
				map[string]any{
					"repo":           rs.path,
					"main_branch":    mb,
					"current_branch": branch,
					"commits":        commits,
					"count":          len(commits),
				})
		}
		if mainHead != "" {
			rs.lastMainHead = mainHead
		}

		// ── Files-changed threshold ────────────────────────────────────────────
		if w.cfg.AlertFilesChanged > 0 {
			files, _ := gitLines(ctx, rs.path, "diff", "--name-only", "origin/"+mb+"...HEAD")
			n := len(files)
			if n > w.cfg.AlertFilesChanged && !rs.filesWarnIssued {
				rs.filesWarnIssued = true
				w.emit(events.EventError,
					fmt.Sprintf("[%s] branch %s has %d files changed (threshold %d) — consider splitting the PR",
						repoLabel, branch, n, w.cfg.AlertFilesChanged),
					map[string]any{
						"repo":          rs.path,
						"branch":        branch,
						"files_changed": n,
						"threshold":     w.cfg.AlertFilesChanged,
						"files":         files,
					})
			} else if n <= w.cfg.AlertFilesChanged {
				rs.filesWarnIssued = false
			}
		}

		// ── Branch-age threshold ───────────────────────────────────────────────
		if w.cfg.AlertBranchAgeDays > 0 {
			mergeBase, err := gitOut(ctx, rs.path, "merge-base", "HEAD", "origin/"+mb)
			if err == nil && mergeBase != "" {
				tsStr, err := gitOut(ctx, rs.path, "log", "--format=%ct", "-1", mergeBase)
				if err == nil && tsStr != "" {
					if ts, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
						ageDays := int(time.Since(time.Unix(ts, 0)).Hours() / 24)
						if ageDays > w.cfg.AlertBranchAgeDays && !rs.ageWarnIssued {
							rs.ageWarnIssued = true
							w.emit(events.EventError,
								fmt.Sprintf("[%s] branch %s is %d days old (threshold %d days) — time to merge or rebase",
									repoLabel, branch, ageDays, w.cfg.AlertBranchAgeDays),
								map[string]any{
									"repo":           rs.path,
									"branch":         branch,
									"age_days":       ageDays,
									"threshold_days": w.cfg.AlertBranchAgeDays,
								})
						} else if ageDays <= w.cfg.AlertBranchAgeDays {
							rs.ageWarnIssued = false
						}
					}
				}
			}
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// expandPath resolves ~ and cleans the path.
func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, p[2:])
	}
	return filepath.Clean(p), nil
}

// discoverRepos returns the repo paths to watch:
//   - If root itself is a git repo, returns [root].
//   - Otherwise scans one level deep for subdirectories that are git repos.
func discoverRepos(root string) ([]string, error) {
	if isGitRepo(root) {
		return []string{root}, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", root, err)
	}
	var repos []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(root, e.Name())
		if isGitRepo(p) {
			repos = append(repos, p)
		}
	}
	return repos, nil
}

func isGitRepo(path string) bool {
	info, err := os.Stat(filepath.Join(path, ".git"))
	return err == nil && info.IsDir()
}

// gitFetch runs a quiet fetch; errors are intentionally ignored (offline is ok).
func gitFetch(ctx context.Context, dir string) {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "fetch", "--quiet", "--no-tags")
	_ = cmd.Run()
}

// gitOut runs a git command and returns trimmed stdout, or an error.
func gitOut(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// gitLines runs a git command and returns non-empty output lines.
func gitLines(ctx context.Context, dir string, args ...string) ([]string, error) {
	out, err := gitOut(ctx, dir, args...)
	if err != nil || out == "" {
		return nil, err
	}
	return strings.Split(out, "\n"), nil
}

// mainBranch returns "main" if it exists on the remote, else "master", else "".
func mainBranch(ctx context.Context, dir string) string {
	for _, b := range []string{"main", "master"} {
		out, err := gitOut(ctx, dir, "rev-parse", "--verify", "origin/"+b)
		if err == nil && out != "" {
			return b
		}
	}
	return ""
}
