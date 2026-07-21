/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// TestTargetFromSpecNamespaceFallback locks in the two resolution paths that
// every cross-namespace code change depends on. A regression here would
// silently break either the legacy same-namespace Trials (nil → trial.Namespace)
// or the TrialSet-generated cross-namespace Trials (set → that value). The
// envtest "should target a Deployment in a different namespace" spec covers
// the override path end-to-end; this covers the nil path that no envtest spec
// names explicitly.
func TestTargetFromSpecNamespaceFallback(t *testing.T) {
	depName := "dep"

	t.Run("nil namespace falls back to trial namespace", func(t *testing.T) {
		trial := &temperv1alpha1.Trial{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
			Spec: temperv1alpha1.TrialSpec{
				Target: temperv1alpha1.Target{
					Kind: "Deployment",
					Name: &depName,
					// Namespace intentionally nil — the legacy / backward-compatible path.
				},
			},
		}

		got := targetFromSpec(trial)

		if got.Namespace != "default" {
			t.Fatalf("nil Target.Namespace: want namespace %q (trial's), got %q", "default", got.Namespace)
		}
		if got.Name != depName {
			t.Fatalf("want name %q, got %q", depName, got.Name)
		}
		if got.Kind != "Deployment" {
			t.Fatalf("want kind Deployment, got %q", got.Kind)
		}
	})

	t.Run("set namespace overrides trial namespace", func(t *testing.T) {
		alt := "alt"
		trial := &temperv1alpha1.Trial{
			// Trial lives in "default" but targets a Deployment in "alt" — the
			// TrialSet-generated cross-namespace path (see trialset-design.md).
			ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
			Spec: temperv1alpha1.TrialSpec{
				Target: temperv1alpha1.Target{
					Kind:      "Deployment",
					Name:      &depName,
					Namespace: &alt,
				},
			},
		}

		got := targetFromSpec(trial)

		if got.Namespace != "alt" {
			t.Fatalf("set Target.Namespace: want %q, got %q", alt, got.Namespace)
		}
	})

	t.Run("set empty-string namespace overrides trial namespace", func(t *testing.T) {
		// A pointer to "" is set, not nil — the override path must take it
		// literally rather than treating it as "unset" and falling back.
		empty := ""
		trial := &temperv1alpha1.Trial{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
			Spec: temperv1alpha1.TrialSpec{
				Target: temperv1alpha1.Target{
					Kind:      "Deployment",
					Name:      &depName,
					Namespace: &empty,
				},
			},
		}

		got := targetFromSpec(trial)

		if got.Namespace != "" {
			t.Fatalf("empty-string Target.Namespace: want %q, got %q", "", got.Namespace)
		}
	})
}
