package risk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// TestAppliesToMapping locks in the rule → scenario prerequisite table from
// the risk-gating design. Changing the mapping in code without updating this
// table fails on purpose: the mapping is a product decision, not an
// implementation detail.
func TestAppliesToMapping(t *testing.T) {
	scenarios := []temperv1alpha1.ScenarioType{
		temperv1alpha1.ScenarioTypePodKill,
		temperv1alpha1.ScenarioTypeNodeDrain,
		temperv1alpha1.ScenarioTypeSpotReclaim,
	}

	// One row per rule; columns follow the scenarios order above.
	want := map[temperv1alpha1.RiskRule][3]bool{
		temperv1alpha1.RiskSingleReplica:         {true, true, true},
		temperv1alpha1.RiskNoPodAntiAffinity:     {false, true, true},
		temperv1alpha1.RiskConcentratedPlacement: {false, true, true},
		temperv1alpha1.RiskMissingReadinessProbe: {true, true, true},
		temperv1alpha1.RiskNoPodDisruptionBudget: {false, true, false},
	}

	require.Len(t, rules, len(want), "mapping table and registry out of sync")

	for _, rule := range rules {
		row, ok := want[rule.ID()]
		require.True(t, ok, "registry rule %q is missing from the mapping table", rule.ID())
		for i, scenario := range scenarios {
			assert.Equal(t, row[i], rule.AppliesTo(scenario), "%s.AppliesTo(%s)", rule.ID(), scenario)
		}
	}
}

func TestRelevant(t *testing.T) {
	both := []temperv1alpha1.Risk{
		{Rule: temperv1alpha1.RiskNoPodDisruptionBudget},
		{Rule: temperv1alpha1.RiskConcentratedPlacement},
	}

	tests := []struct {
		name  string
		risks []temperv1alpha1.Risk
		types []temperv1alpha1.ScenarioType
		want  []temperv1alpha1.RiskRule
	}{
		{
			name:  "spot-reclaim drops the PDB rule",
			risks: both,
			types: []temperv1alpha1.ScenarioType{temperv1alpha1.ScenarioTypeSpotReclaim},
			want:  []temperv1alpha1.RiskRule{temperv1alpha1.RiskConcentratedPlacement},
		},
		{
			name:  "union keeps a risk any listed type needs",
			risks: both,
			types: []temperv1alpha1.ScenarioType{temperv1alpha1.ScenarioTypePodKill, temperv1alpha1.ScenarioTypeNodeDrain},
			want:  []temperv1alpha1.RiskRule{temperv1alpha1.RiskNoPodDisruptionBudget, temperv1alpha1.RiskConcentratedPlacement},
		},
		{
			name:  "unknown token is kept, fail closed",
			risks: []temperv1alpha1.Risk{{Rule: "FutureRule"}},
			types: []temperv1alpha1.ScenarioType{temperv1alpha1.ScenarioTypePodKill},
			want:  []temperv1alpha1.RiskRule{"FutureRule"},
		},
		{
			name:  "nothing relevant returns nil",
			risks: []temperv1alpha1.Risk{{Rule: temperv1alpha1.RiskNoPodDisruptionBudget}},
			types: []temperv1alpha1.ScenarioType{temperv1alpha1.ScenarioTypeSpotReclaim},
			want:  nil,
		},
		{
			name:  "no types returns nil",
			risks: both,
			types: nil,
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Relevant(tc.risks, tc.types)
			if tc.want == nil {
				assert.Nil(t, got, "Relevant() must return nil when nothing is relevant")
				return
			}

			gotRules := make([]temperv1alpha1.RiskRule, 0, len(got))
			for _, r := range got {
				gotRules = append(gotRules, r.Rule)
			}
			assert.Equal(t, tc.want, gotRules)
		})
	}
}
