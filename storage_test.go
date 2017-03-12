package rtvk

import (
	"reflect"
	"testing"
)

func Test_store_snapshot(t *testing.T) {
	tm := map[string]string{"foo": "bar"}
	s := &Storage{store: tm}

	v, _ := s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}

	data, err := s.GetSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.store = nil

	if err := s.recoverFromSnapshot(data); err != nil {
		t.Fatal(err)
	}
	v, _ = s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}
	if !reflect.DeepEqual(s.store, tm) {
		t.Fatalf("store expected %+v, got %+v", tm, s.store)
	}
}
