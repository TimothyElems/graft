// Package store holds branches and runs. v0.2 keeps them in memory; v0.3 moves
// them to Postgres behind the same methods.
package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"graft/kernel/pipeline"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNameTaken   = errors.New("branch name taken")
	ErrBranchLimit = errors.New("branch limit reached")
)

// MaxLiveBranches per project. The contract's 409 example uses this number.
const MaxLiveBranches = 50

type Branch struct {
	ID        string
	Project   string
	Name      string
	Status    string // creating, ready, failed, deleting, deleted
	GitRef    *string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type StepSpec struct {
	ID          string
	Needs       []string
	Image       string
	Command     []string
	MaxAttempts int
	TimeoutMs   int64
	BackoffMs   int64
}

type Run struct {
	ID          string
	BranchID    string
	Status      string // queued, running, succeeded, failed, cancelled
	Steps       []StepSpec
	Concurrency int
	CreatedAt   time.Time
	FinishedAt  *time.Time
	Result      *pipeline.Result // set when the run is finished; nil before
	Err         string           // set if the shell itself failed
}

// Memory is safe for concurrent use. Every getter returns a copy, so a caller
// can never change stored state without going through a method.
type Memory struct {
	mu       sync.Mutex
	branches map[string]Branch
	runs     map[string]Run
}

func NewMemory() *Memory {
	return &Memory{branches: map[string]Branch{}, runs: map[string]Run{}}
}

func live(b Branch) bool { return b.Status != "deleted" && b.Status != "deleting" }

func (m *Memory) CreateBranch(b Branch) (Branch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, other := range m.branches {
		if other.Project != b.Project || !live(other) {
			continue
		}
		if other.Name == b.Name {
			return Branch{}, ErrNameTaken
		}
		n++
	}
	if n >= MaxLiveBranches {
		return Branch{}, ErrBranchLimit
	}
	m.branches[b.ID] = b
	return b, nil
}

func (m *Memory) GetBranch(id string) (Branch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.branches[id]
	if !ok {
		return Branch{}, ErrNotFound
	}
	return b, nil
}

func (m *Memory) CreateRun(r Run) Run {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[r.ID] = r
	return r
}

func (m *Memory) GetRun(id string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return Run{}, ErrNotFound
	}
	return r, nil
}

// UpdateRun applies fn to the stored run under the lock.
func (m *Memory) UpdateRun(id string, fn func(*Run)) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return Run{}, ErrNotFound
	}
	fn(&r)
	m.runs[id] = r
	return r, nil
}

func (m *Memory) QueueDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, r := range m.runs {
		if r.Status == "queued" {
			n++
		}
	}
	return n
}

// RunIDs returns run ids for a branch, oldest first. Used by tests and, later, delete.
func (m *Memory) RunIDs(branchID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Run
	for _, r := range m.runs {
		if r.BranchID == branchID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	ids := make([]string, len(out))
	for i, r := range out {
		ids[i] = r.ID
	}
	return ids
}
