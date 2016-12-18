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
// This file implements parse utility.

package cron

import (
  "fmt"
  "math"
  "strconv"
  "strings"
  "time"
)

// Parse returns a new crontab schedule representing the given spec.
// It returns a descriptive error if the spec is not valid.
//
// It accepts
//   - Full crontab specs, e.g. "* * * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
func Parse(spec string) (_ Schedule, err error) {
  // Convert panics into errors
  defer func() {
    if recovered := recover(); recovered != nil {
      err = fmt.Errorf("%v", recovered)
    }
  }()

  if spec[0] == '@' {
    return parseDescriptor(spec)
  }

  // Split on whitespace.  We require 5 or 6 fields.
  // (second) (minute) (hour) (day of month) (month) (day of week, optional)
  fields := strings.Fields(spec)
  if len(fields) != 5 && len(fields) != 6 {
    return nil, fmt.Errorf("expected 5 or 6 fields, found %d: %s", len(fields),
      spec)
  }

  // If a sixth field is not provided (DayOfWeek), then it is equivalent to star.
  if len(fields) == 5 {
    fields = append(fields, "*")
  }

  second, err := getField(fields[0], seconds)
  if err != nil {
    return nil, err
  }
  minute, err := getField(fields[1], minutes)
  if err != nil {
    return nil, err
  }
  hour, err := getField(fields[2], hours)
  if err != nil {
    return nil, err
  }
  dom, err := getField(fields[3], dom)
  if err != nil {
    return nil, err
  }
  month, err := getField(fields[4], months)
  if err != nil {
    return nil, err
  }
  dow, err := getField(fields[5], dow)
  if err != nil {
    return nil, err
  }

  schedule := &SpecSchedule{
    Second: second,
    Minute: minute,
    Hour:   hour,
    Dom:    dom,
    Month:  month,
    Dow:    dow,
  }

  return schedule, nil
}

// getField returns an Int with the bits set representing all of the times that
// the field represents.  A "field" is a comma-separated list of "ranges".
func getField(field string, r bounds) (uint64, error) {
  // list = range {"," range}
  var bits uint64
  ranges := strings.FieldsFunc(field, func(r rune) bool { return r == ',' })
  for _, expr := range ranges {
    b, err := getRange(expr, r)
    if err != nil {
      return 0, err
    }
    bits |= b
  }
  return bits, nil
}

// getRange returns the bits indicated by the given expression:
//   number | number "-" number [ "/" number ]
func getRange(expr string, r bounds) (uint64, error) {

  var (
    start, end, step uint
    rangeAndStep     = strings.Split(expr, "/")
    lowAndHigh       = strings.Split(rangeAndStep[0], "-")
    singleDigit      = len(lowAndHigh) == 1
    err              error
  )

  var extraStar uint64
  if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
    start = r.min
    end = r.max
    extraStar = starBit
  } else {
    start, err = parseIntOrName(lowAndHigh[0], r.names)
    if err != nil {
      return 0, err
    }
    switch len(lowAndHigh) {
    case 1:
      end = start
    case 2:
      end, err = parseIntOrName(lowAndHigh[1], r.names)
      if err != nil {
        return 0, err
      }
    default:
      return 0, fmt.Errorf("too many hyphens: %s", expr)
    }
  }

  switch len(rangeAndStep) {
  case 1:
    step = 1
  case 2:
    step, err = mustParseInt(rangeAndStep[1])
    if err != nil {
      return 0, err
    }

    // Special handling: "N/step" means "N-max/step".
    if singleDigit {
      end = r.max
    }
  default:
    return 0, fmt.Errorf("too many slashes: %s", expr)
  }

  if start < r.min {
    return 0, fmt.Errorf("beginning of range (%d) below minimum (%d): %s",
      start, r.min, expr)
  }
  if end > r.max {
    return 0, fmt.Errorf("End of range (%d) above maximum (%d): %s",
      end, r.max, expr)
  }
  if start > end {
    return 0, fmt.Errorf("Beginning of range (%d) beyond end of range (%d): %s",
      start, end, expr)
  }

  return getBits(start, end, step) | extraStar, nil
}

// parseIntOrName returns the (possibly-named) integer contained in expr.
func parseIntOrName(expr string, names map[string]uint) (uint, error) {
  if names != nil {
    if namedInt, ok := names[strings.ToLower(expr)]; ok {
      return namedInt, nil
    }
  }
  return mustParseInt(expr)
}

// mustParseInt parses the given expression as an int or panics.
func mustParseInt(expr string) (uint, error) {
  num, err := strconv.Atoi(expr)
  if err != nil {
    return 0, fmt.Errorf("failed to parse int from %s: %s", expr, err)
  }
  if num < 0 {
    return 0, fmt.Errorf("negative number (%d) not allowed: %s", num, expr)
  }

  return uint(num), nil
}

// getBits sets all bits in the range [min, max], modulo the given step size.
func getBits(min, max, step uint) uint64 {
  var bits uint64

  // If step is 1, use shifts.
  if step == 1 {
    return ^(math.MaxUint64 << (max + 1)) & (math.MaxUint64 << min)
  }

  // Else, use a simple loop.
  for i := min; i <= max; i += step {
    bits |= 1 << i
  }
  return bits
}

// all returns all bits within the given bounds.  (plus the star bit)
func all(r bounds) uint64 {
  return getBits(r.min, r.max, 1) | starBit
}

// parseDescriptor returns a pre-defined schedule for the expression, or panics
// if none matches.
func parseDescriptor(spec string) (Schedule, error) {
  switch spec {
  case "@yearly", "@annually":
    return &SpecSchedule{
      Second: 1 << seconds.min,
      Minute: 1 << minutes.min,
      Hour:   1 << hours.min,
      Dom:    1 << dom.min,
      Month:  1 << months.min,
      Dow:    all(dow),
    }, nil

  case "@monthly":
    return &SpecSchedule{
      Second: 1 << seconds.min,
      Minute: 1 << minutes.min,
      Hour:   1 << hours.min,
      Dom:    1 << dom.min,
      Month:  all(months),
      Dow:    all(dow),
    }, nil

  case "@weekly":
    return &SpecSchedule{
      Second: 1 << seconds.min,
      Minute: 1 << minutes.min,
      Hour:   1 << hours.min,
      Dom:    all(dom),
      Month:  all(months),
      Dow:    1 << dow.min,
    }, nil

  case "@daily", "@midnight":
    return &SpecSchedule{
      Second: 1 << seconds.min,
      Minute: 1 << minutes.min,
      Hour:   1 << hours.min,
      Dom:    all(dom),
      Month:  all(months),
      Dow:    all(dow),
    }, nil

  case "@hourly":
    return &SpecSchedule{
      Second: 1 << seconds.min,
      Minute: 1 << minutes.min,
      Hour:   all(hours),
      Dom:    all(dom),
      Month:  all(months),
      Dow:    all(dow),
    }, nil
  }

  const every string = "@every "
  if strings.HasPrefix(spec, every) {
    duration, err := time.ParseDuration(spec[len(every):])
    if err != nil {
      return nil, fmt.Errorf("failed to parse duration %s: %s", spec, err)
    }
    return Every(duration), nil
  }

  return nil, fmt.Errorf("unrecognized descriptor: %s", spec)
}
