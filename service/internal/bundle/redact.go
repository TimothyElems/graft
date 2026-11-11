package bundle

import (
	"regexp"
	"sort"
	"strings"
)

// Redacted replaces every secret the Redactor finds.
const Redacted = "[REDACTED]"

// MinLiteral is the shortest literal secret replaced wherever it appears. A shorter
// one ("graft") would mangle ordinary text; it is still caught where its shape gives
// it away (the password in a URL, PASSWORD=...).
const MinLiteral = 8

// Redactor removes secrets from text: literal values it is told about (the API
// token, the database password) and anything shaped like a credential. It is the
// server's copy of the rules; the Python SDK has the other, and both must pass
// fixtures/redaction/cases.json.
//
// Redaction is best effort. It catches known shapes, not every secret a step might
// print; the bundle's manifest says so, and support asks before opening one.
type Redactor struct {
	literals []string
}

type rule struct {
	re  *regexp.Regexp
	sub func(m []string) string // m: the full match and its groups
}

func keep(n int) func([]string) string { // keep groups 1..n, redact the rest
	return func(m []string) string { return strings.Join(m[1:n+1], "") + Redacted }
}

var rules = []rule{
	{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`),
		func([]string) string { return "[REDACTED PRIVATE KEY]" }},
	// scheme://user:PASSWORD@host
	{regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@"']+:)[^\s@"'/]+(@)`),
		func(m []string) string { return m[1] + Redacted + m[2] }},
	{regexp.MustCompile(`(?i)(authorization:\s*basic\s+)[A-Za-z0-9+/]+=*`), keep(1)},
	{regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}`), keep(1)},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}`), keep(0)}, // JWT
	{regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{30,}`), keep(0)},                                 // GitHub
	{regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`), keep(0)},                                // AWS key id
	{regexp.MustCompile(`\bsb_secret_[A-Za-z0-9_-]{8,}`), keep(0)},                                // Supabase secret key
	// NAME=value, NAME: value, "name": "value" where NAME ends in a credential word
	{regexp.MustCompile(`(?i)\b([a-z0-9_]*(?:password|passwd|pwd|secret|token|api_?key|access_?key))("?\s*[=:]\s*"?)([^\s"'&,;]+)`),
		keep(2)}, // an already redacted value is replaced by itself: no change, not counted
}

func NewRedactor(secrets ...string) Redactor {
	var lit []string
	for _, s := range secrets {
		if len(s) >= MinLiteral {
			lit = append(lit, s)
		}
	}
	sort.Slice(lit, func(i, j int) bool { return len(lit[i]) > len(lit[j]) }) // longest first
	return Redactor{literals: lit}
}

// Redact returns s without secrets and how many it removed.
func (r Redactor) Redact(s string) (string, int) {
	n := 0
	for _, lit := range r.literals {
		if c := strings.Count(s, lit); c > 0 {
			n += c
			s = strings.ReplaceAll(s, lit, Redacted)
		}
	}
	for _, ru := range rules {
		s = ru.re.ReplaceAllStringFunc(s, func(match string) string {
			out := ru.sub(ru.re.FindStringSubmatch(match))
			if out != match {
				n++
			}
			return out
		})
	}
	return s, n
}
