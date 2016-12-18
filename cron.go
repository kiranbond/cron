// Copyright (c) 2016 ZeroStack, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This library implements a cron spec parser and runner.  See the README for
// more details.

package cron

import (
  "fmt"
  "runtime"
  "sort"
  "time"

  "code.google.com/p/go-uuid/uuid"
  "github.com/golang/glog"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
  entries  []*Entry
  start    chan struct{}
  stop     chan struct{}
  add      chan *Entry
  del      chan string
  err      chan error
  snapshot chan []*Entry
  running  bool
}

// Job is an interface for submitted cron jobs.
type Job interface {
  Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
  // Return the next activation time, later than the given time.
  // Next is invoked initially, and then each time the job is run.
  Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
  // The schedule on which this job should be run.
  Schedule Schedule

  // The next time the job will run. This is the zero time if Cron has not been
  // started or this entry's schedule is unsatisfiable
  Next time.Time

  // The last time this job was run. This is the zero time if the job has never
  // been run.
  Prev time.Time

  // The Job to run.
  Job Job

  // The Job ID.
  ID string
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
  // Two zero times should return false.
  // Otherwise, zero is "greater" than any other time.
  // (To sort it at the end of the list.)
  if s[i].Next.IsZero() {
    return false
  }
  if s[j].Next.IsZero() {
    return true
  }
  return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner.
func New() *Cron {
  c := &Cron{
    entries:  nil,
    add:      make(chan *Entry),
    del:      make(chan string),
    err:      make(chan error),
    start:    make(chan struct{}),
    stop:     make(chan struct{}),
    snapshot: make(chan []*Entry),
    running:  false,
  }
  go c.run()
  return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job.
type FuncJob func()

// Run invokes the function.
func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func()) (string, error) {
  return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) (string, error) {
  schedule, err := Parse(spec)
  if err != nil {
    return "", err
  }
  id := c.Schedule(schedule, cmd)
  return id, nil
}

// DeleteJob deletes a Job from the Cron.
func (c *Cron) DeleteJob(id string) error {
  c.del <- id
  err := <-c.err
  return err
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job) string {
  id := uuid.New()
  entry := &Entry{
    Schedule: schedule,
    Job:      cmd,
    ID:       id,
  }
  c.add <- entry
  return id
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
  c.snapshot <- nil
  x := <-c.snapshot
  return x
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
  c.start <- struct{}{}
}

func (c *Cron) runWithRecovery(j Job) {
  defer func() {
    if r := recover(); r != nil {
      const size = 64 << 10
      buf := make([]byte, size)
      buf = buf[:runtime.Stack(buf, false)]
      glog.Warningf("cron: panic running job: %v\n%s", r, buf)
    }
  }()
  j.Run()
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
  // Figure out the next activation times for each entry.
  now := time.Now().Local()
  for _, entry := range c.entries {
    entry.Next = entry.Schedule.Next(now)
  }

  for {
    // Determine the next entry to run.
    sort.Sort(byTime(c.entries))

    var effective time.Time
    if !c.running || len(c.entries) == 0 || c.entries[0].Next.IsZero() {
      // If there are no entries yet, just sleep - it still handles new entries
      // and stop requests.
      effective = now.AddDate(10, 0, 0)
    } else {
      effective = c.entries[0].Next
    }

    select {
    case now = <-time.After(effective.Sub(now)):
      // Run every entry whose next time was this effective time.
      for _, e := range c.entries {
        if e.Next != effective {
          break
        }
        go c.runWithRecovery(e.Job)
        e.Prev = e.Next
        e.Next = e.Schedule.Next(effective)
      }
      continue

    case newEntry := <-c.add:
      c.entries = append(c.entries, newEntry)
      newEntry.Next = newEntry.Schedule.Next(time.Now().Local())

    case deleteID := <-c.del:
      c.err <- c.deleteEntry(deleteID)

    case <-c.snapshot:
      c.snapshot <- c.entrySnapshot()

    case <-c.start:
      c.running = true

    case <-c.stop:
      c.running = false
    }

    // 'now' should be updated after newEntry and snapshot cases.
    now = time.Now().Local()
  }
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
  c.stop <- struct{}{}
}

func (c *Cron) deleteEntry(id string) error {
  for idx, entry := range c.entries {
    if entry.ID == id {
      c.entries = append(c.entries[:idx], c.entries[idx+1:]...)
      return nil
    }
  }
  return fmt.Errorf("no job with id %s found", id)
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
  entries := []*Entry{}
  for _, e := range c.entries {
    entries = append(entries, &Entry{
      Schedule: e.Schedule,
      Next:     e.Next,
      Prev:     e.Prev,
      Job:      e.Job,
    })
  }
  return entries
}
