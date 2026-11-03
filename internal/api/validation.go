package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
)

var resourceName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// DecodeStrict rejects unknown fields and additional JSON values.
func DecodeStrict(r io.Reader, dst any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validateMetadata(m Metadata) error {
	if len(m.Name) == 0 || len(m.Name) > 63 || !resourceName.MatchString(m.Name) {
		return errors.New("name must be a lowercase DNS label of at most 63 characters")
	}
	if m.Namespace != "" && m.Namespace != "default" {
		return errors.New("only the default namespace is supported")
	}
	return nil
}

func validatePodSpec(s PodSpec) error {
	if s.Image == "" {
		return errors.New("image is required")
	}
	if s.RestartPolicy != "Always" {
		return errors.New("restartPolicy must be Always")
	}
	r, l := s.Resources.Requests, s.Resources.Limits
	if r.CPUMilli <= 0 || r.MemoryBytes <= 0 || l.CPUMilli <= 0 || l.MemoryBytes <= 0 {
		return errors.New("CPU and memory requests and limits must be positive")
	}
	if r.CPUMilli > l.CPUMilli || r.MemoryBytes > l.MemoryBytes {
		return errors.New("requests must not exceed limits")
	}
	if s.Readiness != nil && (s.Readiness.Port < 1 || s.Readiness.Port > 65535) {
		return errors.New("readiness TCP port must be between 1 and 65535")
	}
	return nil
}

func ValidatePod(p Pod) error {
	if p.APIVersion != Version || p.Kind != "Pod" {
		return errors.New("invalid apiVersion or kind for Pod")
	}
	if err := validateMetadata(p.Metadata); err != nil {
		return err
	}
	return validatePodSpec(p.Spec)
}

func ValidateReplicaSet(rs ReplicaSet) error {
	if rs.APIVersion != Version || rs.Kind != "ReplicaSet" {
		return errors.New("invalid apiVersion or kind for ReplicaSet")
	}
	if err := validateMetadata(rs.Metadata); err != nil {
		return err
	}
	if rs.Spec.Replicas < 0 || len(rs.Spec.Selector) == 0 {
		return errors.New("replicas must be nonnegative and selector must not be empty")
	}
	for k, v := range rs.Spec.Selector {
		if actual, ok := rs.Spec.Template.Labels[k]; !ok || actual != v {
			return fmt.Errorf("template does not match selector key %q", k)
		}
	}
	return validatePodSpec(rs.Spec.Template.Spec)
}

// ValidatePodUpdate checks immutable fields; authorization and CAS are separate.
func ValidatePodUpdate(old, next Pod) error {
	if err := ValidatePod(next); err != nil {
		return err
	}
	oldMeta, nextMeta := old.Metadata, next.Metadata
	oldMeta.Annotations, nextMeta.Annotations = nil, nil
	oldMeta.ResourceVersion, nextMeta.ResourceVersion = 0, 0
	if !reflect.DeepEqual(oldMeta, nextMeta) || !reflect.DeepEqual(old.Spec, next.Spec) || old.NodeUID != next.NodeUID {
		return errors.New("Pod spec, labels, ownership, binding and system metadata are immutable")
	}
	return nil
}

func ValidateReplicaSetUpdate(old, next ReplicaSet) error {
	if err := ValidateReplicaSet(next); err != nil {
		return err
	}
	if !reflect.DeepEqual(old.Spec.Selector, next.Spec.Selector) || !reflect.DeepEqual(old.Spec.Template, next.Spec.Template) {
		return errors.New("ReplicaSet selector and template are immutable")
	}
	return nil
}
