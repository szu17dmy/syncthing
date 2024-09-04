// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:generate go run ../../proto/scripts/protofmt.go database.proto
//go:generate protoc -I ../../ -I . --gogofast_out=. database.proto

package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/sliceutil"
)

type clock interface {
	Now() time.Time
}

type defaultClock struct{}

func (defaultClock) Now() time.Time {
	return time.Now()
}

type database interface {
	put(key string, rec DatabaseRecord) error
	merge(key string, addrs []DatabaseAddress, seen int64) error
	get(key string) (DatabaseRecord, error)
}

type inMemoryStore struct {
	m             sync.Map
	dir           string
	flushInterval time.Duration
	clock         clock
}

func newLevelDBStore(dir string, flushInterval time.Duration) *inMemoryStore {
	s := &inMemoryStore{
		dir:           dir,
		flushInterval: flushInterval,
		clock:         defaultClock{},
	}
	if err := s.read(); err != nil {
		log.Printf("Error reading database: %v", err)
	}
	return s
}

func (s *inMemoryStore) put(key string, rec DatabaseRecord) error {
	t0 := time.Now()
	s.m.Store(key, rec)
	databaseOperations.WithLabelValues(dbOpPut, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpPut).Observe(time.Since(t0).Seconds())
	return nil
}

func (s *inMemoryStore) merge(key string, addrs []DatabaseAddress, seen int64) error {
	t0 := time.Now()

	newRec := DatabaseRecord{
		Addresses: addrs,
		Seen:      seen,
	}

	var oldRec DatabaseRecord
	if vi, ok := s.m.Load(key); ok {
		oldRec = vi.(DatabaseRecord)
	}
	newRec = merge(newRec, oldRec)
	s.m.Store(key, newRec)

	databaseOperations.WithLabelValues(dbOpMerge, dbResSuccess).Inc()
	databaseOperationSeconds.WithLabelValues(dbOpMerge).Observe(time.Since(t0).Seconds())

	return nil
}

func (s *inMemoryStore) get(key string) (DatabaseRecord, error) {
	t0 := time.Now()
	defer func() {
		databaseOperationSeconds.WithLabelValues(dbOpGet).Observe(time.Since(t0).Seconds())
	}()

	vi, ok := s.m.Load(key)
	if !ok {
		databaseOperations.WithLabelValues(dbOpGet, dbResNotFound).Inc()
		return DatabaseRecord{}, nil
	}

	rec := vi.(DatabaseRecord)
	rec.Addresses = expire(rec.Addresses, s.clock.Now().UnixNano())
	databaseOperations.WithLabelValues(dbOpGet, dbResSuccess).Inc()
	return rec, nil
}

func (s *inMemoryStore) Serve(ctx context.Context) error {
	t := time.NewTimer(s.flushInterval)
	defer t.Stop()

	if s.flushInterval <= 0 {
		t.Stop()
	}

loop:
	for {
		select {
		case <-t.C:
			if err := s.write(); err != nil {
				log.Println("Error writing database:", err)
			}
			s.calculateStatistics()
			t.Reset(s.flushInterval)

		case <-ctx.Done():
			// We're done.
			break loop
		}
	}

	return s.write()
}

func (s *inMemoryStore) calculateStatistics() {
	t0 := time.Now()
	nowNanos := t0.UnixNano()
	cutoff24h := t0.Add(-24 * time.Hour).UnixNano()
	cutoff1w := t0.Add(-7 * 24 * time.Hour).UnixNano()
	current, currentIPv4, currentIPv6, last24h, last1w, errors := 0, 0, 0, 0, 0, 0

	s.m.Range(func(keyI, valueI any) bool {
		rec := valueI.(DatabaseRecord)

		// If there are addresses that have not expired it's a current
		// record, otherwise account it based on when it was last seen
		// (last 24 hours or last week) or finally as inactice.
		addrs := expire(rec.Addresses, nowNanos)
		switch {
		case len(addrs) > 0:
			current++
			seenIPv4, seenIPv6 := false, false
			for _, addr := range addrs {
				uri, err := url.Parse(addr.Address)
				if err != nil {
					continue
				}
				host, _, err := net.SplitHostPort(uri.Host)
				if err != nil {
					continue
				}
				if ip := net.ParseIP(host); ip != nil && ip.To4() != nil {
					seenIPv4 = true
				} else if ip != nil {
					seenIPv6 = true
				}
				if seenIPv4 && seenIPv6 {
					break
				}
			}
			if seenIPv4 {
				currentIPv4++
			}
			if seenIPv6 {
				currentIPv6++
			}
		case rec.Seen > cutoff24h:
			last24h++
		case rec.Seen > cutoff1w:
			last1w++
		default:
			// drop the record if it's older than a week
			s.m.Delete(keyI)
		}
		return true
	})

	databaseKeys.WithLabelValues("current").Set(float64(current))
	databaseKeys.WithLabelValues("currentIPv4").Set(float64(currentIPv4))
	databaseKeys.WithLabelValues("currentIPv6").Set(float64(currentIPv6))
	databaseKeys.WithLabelValues("last24h").Set(float64(last24h))
	databaseKeys.WithLabelValues("last1w").Set(float64(last1w))
	databaseKeys.WithLabelValues("error").Set(float64(errors))
	databaseStatisticsSeconds.Set(time.Since(t0).Seconds())
}

func (s *inMemoryStore) write() (err error) {
	t0 := time.Now()
	defer func() {
		databaseWriteSeconds.Add(time.Since(t0).Seconds())
		if err == nil {
			databaseLastWritten.Set(float64(t0.Unix()))
		}
	}()

	dbf := path.Join(s.dir, "records.db")
	fd, err := os.Create(dbf + ".tmp")
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(fd)

	var buf []byte
	var rangeErr error
	now := s.clock.Now().UnixNano()
	cutoff1w := s.clock.Now().Add(-7 * 24 * time.Hour).UnixNano()
	s.m.Range(func(keyI, valueI any) bool {
		key := keyI.(string)
		value := valueI.(DatabaseRecord)
		if value.Seen < cutoff1w {
			// drop the record if it's older than a week
			return true
		}
		rec := ReplicationRecord{
			Key:       key,
			Addresses: expire(value.Addresses, now),
			Seen:      value.Seen,
		}
		s := rec.Size()
		if s+4 > len(buf) {
			buf = make([]byte, s+4)
		}
		n, err := rec.MarshalTo(buf[4:])
		if err != nil {
			rangeErr = err
			return false
		}
		binary.BigEndian.PutUint32(buf, uint32(n))
		if _, err := bw.Write(buf[:n+4]); err != nil {
			rangeErr = err
			return false
		}
		return true
	})
	if rangeErr != nil {
		_ = fd.Close()
		return rangeErr
	}

	if err := bw.Flush(); err != nil {
		_ = fd.Close
		return err
	}
	if err := fd.Close(); err != nil {
		return err
	}
	return os.Rename(dbf+".tmp", dbf)
}

func (s *inMemoryStore) read() error {
	fd, err := os.Open(path.Join(s.dir, "records.db"))
	if err != nil {
		return err
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	var buf []byte
	for {
		var n uint32
		if err := binary.Read(br, binary.BigEndian, &n); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if int(n) > len(buf) {
			buf = make([]byte, n)
		}
		if _, err := io.ReadFull(br, buf[:n]); err != nil {
			return err
		}
		rec := ReplicationRecord{}
		if err := rec.Unmarshal(buf[:n]); err != nil {
			return err
		}
		s.m.Store(rec.Key, DatabaseRecord{
			Addresses: rec.Addresses,
			Seen:      rec.Seen,
		})
	}
	return nil
}

// merge returns the merged result of the two database records a and b. The
// result is the union of the two address sets, with the newer expiry time
// chosen for any duplicates.
func merge(a, b DatabaseRecord) DatabaseRecord {
	// Both lists must be sorted for this to work.
	if !sort.IsSorted(databaseAddressOrder(a.Addresses)) {
		log.Println("Warning: bug: addresses not correctly sorted in merge")
		a.Addresses = sortedAddressCopy(a.Addresses)
	}
	if !sort.IsSorted(databaseAddressOrder(b.Addresses)) {
		// no warning because this is the side we read from disk and it may
		// legitimately predate correct sorting.
		b.Addresses = sortedAddressCopy(b.Addresses)
	}

	res := DatabaseRecord{
		Addresses: make([]DatabaseAddress, 0, len(a.Addresses)+len(b.Addresses)),
		Seen:      a.Seen,
	}
	if b.Seen > a.Seen {
		res.Seen = b.Seen
	}

	aIdx := 0
	bIdx := 0
	aAddrs := a.Addresses
	bAddrs := b.Addresses
loop:
	for {
		switch {
		case aIdx == len(aAddrs) && bIdx == len(bAddrs):
			// both lists are exhausted, we are done
			break loop

		case aIdx == len(aAddrs):
			// a is exhausted, pick from b and continue
			res.Addresses = append(res.Addresses, bAddrs[bIdx])
			bIdx++
			continue

		case bIdx == len(bAddrs):
			// b is exhausted, pick from a and continue
			res.Addresses = append(res.Addresses, aAddrs[aIdx])
			aIdx++
			continue
		}

		// We have values left on both sides.
		aVal := aAddrs[aIdx]
		bVal := bAddrs[bIdx]

		switch {
		case aVal.Address == bVal.Address:
			// update for same address, pick newer
			if aVal.Expires > bVal.Expires {
				res.Addresses = append(res.Addresses, aVal)
			} else {
				res.Addresses = append(res.Addresses, bVal)
			}
			aIdx++
			bIdx++

		case aVal.Address < bVal.Address:
			// a is smallest, pick it and continue
			res.Addresses = append(res.Addresses, aVal)
			aIdx++

		default:
			// b is smallest, pick it and continue
			res.Addresses = append(res.Addresses, bVal)
			bIdx++
		}
	}
	return res
}

// expire returns the list of addresses after removing expired entries.
// Expiration happen in place, so the slice given as the parameter is
// destroyed. Internal order is not preserved.
func expire(addrs []DatabaseAddress, now int64) []DatabaseAddress {
	i := 0
	for i < len(addrs) {
		if addrs[i].Expires < now {
			addrs = sliceutil.RemoveAndZero(addrs, i)
			continue
		}
		i++
	}
	return addrs
}

func sortedAddressCopy(addrs []DatabaseAddress) []DatabaseAddress {
	sorted := make([]DatabaseAddress, len(addrs))
	copy(sorted, addrs)
	sort.Sort(databaseAddressOrder(sorted))
	return sorted
}

type databaseAddressOrder []DatabaseAddress

func (s databaseAddressOrder) Less(a, b int) bool {
	return s[a].Address < s[b].Address
}

func (s databaseAddressOrder) Swap(a, b int) {
	s[a], s[b] = s[b], s[a]
}

func (s databaseAddressOrder) Len() int {
	return len(s)
}
