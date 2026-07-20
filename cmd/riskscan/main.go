// Command riskscan reports resilience risks on workloads in a namespace.
//
// Risk detection is read-only, cheap and instant, while a Trial injects a real
// fault. Binding the two would mean the only way to learn a workload is fragile
// is to disrupt it, so this is the non-destructive door to the same rules the
// Trial controller records in status.
//
// It scans Deployments and StatefulSets. StatefulSets cannot be Trial targets
// yet — the scenarios only inject into Deployments — but the rules read them
// fine, so they are still worth linting.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/risk"
)

// target is one workload to lint.
type target struct {
	kind string
	name string
}

func main() {
	var namespace string
	var details bool
	var failOnRisk bool
	flag.StringVar(&namespace, "namespace", "default", "Namespace to scan.")
	flag.BoolVar(&details, "details", false, "Print the full message for every risk.")
	flag.BoolVar(&failOnRisk, "fail-on-risk", false, "Exit 1 when any risk is found (for CI).")
	flag.Parse()

	risky, err := run(context.Background(), namespace, details)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if risky && failOnRisk {
		os.Exit(1)
	}
}

// run scans the namespace and prints one row per workload. It reports whether
// any risk was found.
func run(ctx context.Context, namespace string, details bool) (bool, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(temperv1alpha1.AddToScheme(scheme))

	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return false, fmt.Errorf("build client: %w", err)
	}

	targets, err := listTargets(ctx, c, namespace)
	if err != nil {
		return false, err
	}
	if len(targets) == 0 {
		fmt.Printf("No Deployments or StatefulSets in namespace %q.\n", namespace)
		return false, nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "KIND\tNAME\tRISKS")

	var detailed []string
	found := false

	for _, t := range targets {
		risks, err := risk.Detect(ctx, c, t.kind, namespace, t.name)
		if err != nil {
			return false, fmt.Errorf("scan %s %s: %w", t.kind, t.name, err)
		}

		if len(risks) == 0 {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", t.kind, t.name, "-")
			continue
		}
		found = true

		rules := make([]string, len(risks))
		for i, rk := range risks {
			rules[i] = string(rk.Rule)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", t.kind, t.name, strings.Join(rules, ","))

		if details {
			for _, rk := range risks {
				detailed = append(detailed, fmt.Sprintf("%s/%s  %s: %s", t.kind, t.name, rk.Rule, rk.Message))
			}
		}
	}

	if err := w.Flush(); err != nil {
		return found, fmt.Errorf("write output: %w", err)
	}
	for _, line := range detailed {
		fmt.Printf("\n%s\n", line)
	}

	return found, nil
}

// listTargets returns every Deployment and StatefulSet in the namespace.
func listTargets(ctx context.Context, c client.Client, namespace string) ([]target, error) {
	var targets []target

	var deps appsv1.DeploymentList
	if err := c.List(ctx, &deps, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	for i := range deps.Items {
		targets = append(targets, target{kind: "Deployment", name: deps.Items[i].Name})
	}

	var sets appsv1.StatefulSetList
	if err := c.List(ctx, &sets, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list statefulsets: %w", err)
	}
	for i := range sets.Items {
		targets = append(targets, target{kind: "StatefulSet", name: sets.Items[i].Name})
	}

	return targets, nil
}
