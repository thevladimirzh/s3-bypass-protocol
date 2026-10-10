package s3

import "testing"

// Rule IDs are built by collapsing path separators to dashes, and SetupLifecycle
// reconciles by ID. That makes the mapping many-to-one: "team-a/prod/" and
// "team-a-prod/" both produce "fedarisha-expire-team-a-prod".
//
// The second inbound to start therefore sees a rule with a colliding ID, skips
// it as if it were its own, and writes back a configuration that no longer
// contains it. The first prefix is left with no expiry at all — and since
// orphans are otherwise permanent (a client that dies mid-transfer leaves its
// files, and cleanupSession returns silently when List fails), those objects
// are retained and billed forever with no code path that recovers them.
//
// The multi-user layout puts prefixes exactly one path segment apart, so this
// is reachable with ordinary configuration rather than contrived input.
func TestLifecycleRuleIDIsInjective(t *testing.T) {
	collisions := [][2]string{
		{"team-a/prod/", "team-a-prod/"},
		{"a/b/", "a-b/"},
		{"a/b/", "a/b/c/"},
		{"a/", "a/b/"},
		{"users/alice/sessions/", "users-alice-sessions/"},
	}

	for _, pair := range collisions {
		t.Run(pair[0]+" vs "+pair[1], func(t *testing.T) {
			if a, b := lifecycleRuleIDForPrefix(pair[0]), lifecycleRuleIDForPrefix(pair[1]); a == b {
				t.Errorf("prefixes %q and %q both produce rule ID %q — one of them loses its expiry rule",
					pair[0], pair[1], a)
			}
		})
	}
}

// Distinct prefixes must still get distinct IDs — the encoding may change, the
// property may not.
func TestLifecycleRuleIDIsStableAndDistinct(t *testing.T) {
	prefixes := []string{
		"", "a/", "a/b/", "a/b/c/", "team-a/prod/", "team_a/", "9001/",
		"проект/", "a b/", "a//b/",
	}
	seen := map[string]string{}
	for _, p := range prefixes {
		id := lifecycleRuleIDForPrefix(p)
		if id == "" {
			t.Errorf("prefix %q produced an empty rule ID", p)
			continue
		}
		if other, dup := seen[id]; dup {
			t.Errorf("prefixes %q and %q collide on rule ID %q", other, p, id)
		}
		seen[id] = p

		if got := lifecycleRuleIDForPrefix(p); got != id {
			t.Errorf("prefix %q is not stable: %q then %q", p, id, got)
		}
	}
}

// VK Cloud accepts alphanumerics plus -_. — the encoding has to stay inside
// that, because a rejected rule ID means no rule at all rather than a
// differently-named one.
func TestLifecycleRuleIDUsesOnlyAcceptedCharacters(t *testing.T) {
	for _, p := range []string{"", "a/", "a/b/", "проект/", "a b/", "a//b/", "a-b/"} {
		id := lifecycleRuleIDForPrefix(p)
		for _, r := range id {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			case r == '-', r == '_', r == '.':
			default:
				t.Errorf("prefix %q produced rule ID %q containing %q, which VK Cloud rejects", p, id, r)
			}
		}
	}
}
