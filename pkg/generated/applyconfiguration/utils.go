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

// Code generated by applyconfiguration-gen. DO NOT EDIT.

package applyconfiguration

import (
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	v1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/generated/applyconfiguration/scheduling/v1alpha1"
)

// ForKind returns an apply configuration type for the given GroupVersionKind, or nil if no
// apply configuration type exists for the given GroupVersionKind.
func ForKind(kind schema.GroupVersionKind) interface{} {
	switch kind {
	// Group=scheduling.x-k8s.io, Version=v1alpha1
	case v1alpha1.SchemeGroupVersion.WithKind("ElasticQuota"):
		return &schedulingv1alpha1.ElasticQuotaApplyConfiguration{}
	case v1alpha1.SchemeGroupVersion.WithKind("ElasticQuotaSpec"):
		return &schedulingv1alpha1.ElasticQuotaSpecApplyConfiguration{}
	case v1alpha1.SchemeGroupVersion.WithKind("ElasticQuotaStatus"):
		return &schedulingv1alpha1.ElasticQuotaStatusApplyConfiguration{}
	case v1alpha1.SchemeGroupVersion.WithKind("PodGroup"):
		return &schedulingv1alpha1.PodGroupApplyConfiguration{}
	case v1alpha1.SchemeGroupVersion.WithKind("PodGroupSpec"):
		return &schedulingv1alpha1.PodGroupSpecApplyConfiguration{}
	case v1alpha1.SchemeGroupVersion.WithKind("PodGroupStatus"):
		return &schedulingv1alpha1.PodGroupStatusApplyConfiguration{}

	}
	return nil
}