package scenarios

import (
	"fmt"
	"net/url"
	"sort"
)

type Scenario struct {
	Name  string
	Path  string
	Query map[string]string
}

func (s Scenario) URL(base string) (string, error) {
	parsed, err := url.Parse(base + s.Path)
	if err != nil {
		return "", err
	}
	q := parsed.Query()
	q.Set("scenario", s.Name)
	for k, v := range s.Query {
		q.Set(k, v)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

var all = []Scenario{
	{Name: "browse", Path: "/browse"},
	{Name: "inventory-http", Path: "/inventory", Query: map[string]string{"transport": "http"}},
	{Name: "inventory-grpc", Path: "/inventory", Query: map[string]string{"transport": "grpc"}},
	{Name: "checkout-http", Path: "/checkout", Query: map[string]string{"transport": "http", "rollback": "false"}},
	{Name: "checkout-grpc", Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "false"}},
	{Name: "checkout-rollback-grpc", Path: "/checkout", Query: map[string]string{"transport": "grpc", "rollback": "true"}},
	{Name: "checkout-http-error", Path: "/checkout", Query: map[string]string{"transport": "http", "fail": "true"}},
	{Name: "checkout-grpc-error", Path: "/checkout", Query: map[string]string{"transport": "grpc"}},
	{Name: "wide", Path: "/wide"},
	{Name: "deep", Path: "/deep"},
	{Name: "checkout-async-joined", Path: "/checkout", Query: map[string]string{"transport": "grpc", "notify": "joined"}},
	{Name: "checkout-async-detached", Path: "/checkout", Query: map[string]string{"transport": "grpc", "notify": "detached"}},
	{Name: "catalog-db", Path: "/browse", Query: map[string]string{"source": "db"}},
	{Name: "browse-cached", Path: "/browse", Query: map[string]string{"cache": "true"}},
}

var byName = func() map[string]Scenario {
	m := make(map[string]Scenario, len(all))
	for _, s := range all {
		m[s.Name] = s
	}
	return m
}()

func All() []Scenario {
	out := make([]Scenario, len(all))
	copy(out, all)
	return out
}

func Get(name string) (Scenario, bool) {
	s, ok := byName[name]
	return s, ok
}

func Names() []string {
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

func Validate(name string) error {
	if _, ok := byName[name]; !ok {
		return fmt.Errorf("unknown scenario %q", name)
	}
	return nil
}
