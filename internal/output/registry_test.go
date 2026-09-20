package output

import "testing"

type fakeOutput struct {
	writes []Record
}

func (f *fakeOutput) Init(ScanInfo, []string) error { return nil }
func (f *fakeOutput) Write(rec Record) error        { f.writes = append(f.writes, rec); return nil }
func (f *fakeOutput) Clean() error                  { return nil }
func (f *fakeOutput) Description() string           { return "fake" }
func (f *fakeOutput) Arguments() []string           { return nil }

func TestRegisterAndNew(t *testing.T) {
	name := "output_registry_test_fake"
	Register(name, func() Output { return &fakeOutput{} })

	o, ok := New(name)
	if !ok {
		t.Fatalf("New(%q) not found after Register", name)
	}
	if o.Description() != "fake" {
		t.Fatalf("New(%q).Description() = %q, want %q", name, o.Description(), "fake")
	}

	found := false
	for _, n := range Names() {
		if n == name {
			found = true
		}
	}
	if !found {
		t.Fatalf("Names() does not contain %q", name)
	}
}

func TestNewReturnsFreshInstances(t *testing.T) {
	name := "output_registry_test_fresh"
	Register(name, func() Output { return &fakeOutput{} })

	a, _ := New(name)
	b, _ := New(name)
	_ = a.Write(Record{Module: "m"})
	fa := a.(*fakeOutput)
	fb := b.(*fakeOutput)
	if len(fa.writes) != 1 || len(fb.writes) != 0 {
		t.Fatalf("New did not return independent instances: a=%d writes, b=%d writes", len(fa.writes), len(fb.writes))
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	name := "output_registry_test_dup"
	Register(name, func() Output { return &fakeOutput{} })

	defer func() {
		if recover() == nil {
			t.Fatalf("Register(%q) a second time did not panic", name)
		}
	}()
	Register(name, func() Output { return &fakeOutput{} })
}
