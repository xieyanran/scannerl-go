package fpmodule

import "testing"

type fakeModule struct{}

func (fakeModule) DefaultConfig() Config { return Config{Port: 1, Transport: TCP} }
func (fakeModule) Description() string   { return "fake" }
func (fakeModule) Arguments() []string   { return nil }
func (fakeModule) Next(Input) Step       { return OK("fake") }

func TestRegisterAndGet(t *testing.T) {
	name := "fpmodule_registry_test_fake"
	Register(name, fakeModule{})

	m, ok := Get(name)
	if !ok {
		t.Fatalf("Get(%q) not found after Register", name)
	}
	if m.Description() != "fake" {
		t.Fatalf("Get(%q).Description() = %q, want %q", name, m.Description(), "fake")
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

func TestRegisterDuplicatePanics(t *testing.T) {
	name := "fpmodule_registry_test_dup"
	Register(name, fakeModule{})

	defer func() {
		if recover() == nil {
			t.Fatalf("Register(%q) a second time did not panic", name)
		}
	}()
	Register(name, fakeModule{})
}

func TestGetUnknown(t *testing.T) {
	if _, ok := Get("fpmodule_registry_test_does_not_exist"); ok {
		t.Fatalf("Get returned ok=true for an unregistered name")
	}
}
