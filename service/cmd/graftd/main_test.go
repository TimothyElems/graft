package main

import (
	"reflect"
	"testing"
)

func TestDBSecretsIsThePasswordOnly(t *testing.T) {
	for in, want := range map[string][]string{
		"postgres://graft:pa55word@db:5432/graft": {"pa55word"},
		"postgres://graft@db:5432/graft":          nil,
		"host=db password=x user=graft":           nil, // not a URL: the bundle's patterns handle it
	} {
		if got := dbSecrets(in); !reflect.DeepEqual(got, want) {
			t.Errorf("dbSecrets(%q) = %v, want %v", in, got, want)
		}
	}
}
