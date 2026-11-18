/*
Copyright The Kubernetes Authors.

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

package cache

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRejectedPodKey(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pending"}}
	if got := rejectedPodKey(pod); got != "ns/pending" {
		t.Fatalf("rejectedPodKey = %q, want ns/pending", got)
	}
}

func TestNamespacedPodFromKey(t *testing.T) {
	ns, name := NamespacedPodFromKey("ns/pending")
	if ns != "ns" || name != "pending" {
		t.Fatalf("got (%q, %q), want (ns, pending)", ns, name)
	}
	ns, name = NamespacedPodFromKey("pending")
	if ns != "" || name != "" {
		t.Fatalf("bad key without slash: got (%q, %q)", ns, name)
	}
	ns, name = NamespacedPodFromKey("ns/")
	if ns != "" || name != "" {
		t.Fatalf("empty name: got (%q, %q)", ns, name)
	}
	ns, name = NamespacedPodFromKey("ns/foo/bar")
	if ns != "ns" || name != "foo/bar" {
		t.Fatalf("extra slash: got (%q, %q)", ns, name)
	}
}
