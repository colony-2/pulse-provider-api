package plugin

import (
	"bytes"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// DecodeConfig decodes one strict YAML mapping without interpolation. Aliases,
// merge keys, duplicates, unknown fields, scalar coercions and custom unmarshaling
// are rejected, including within collections. Errors contain no input values.
func DecodeConfig(data []byte, destination any) error {
	if len(data) > MaxConfigBytes {
		return ErrTooLarge
	}
	v := reflect.ValueOf(destination)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return ErrInvalid
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if e := d.Decode(&doc); e != nil {
		return fmt.Errorf("%w: YAML mapping required", ErrInvalid)
	}
	var extra yaml.Node
	if e := d.Decode(&extra); e != io.EOF {
		return fmt.Errorf("%w: single YAML document required", ErrInvalid)
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return ErrInvalid
	}
	count := 0
	if e := checkNode(doc.Content[0], 0, &count); e != nil {
		return e
	}
	if e := checkType(doc.Content[0], v.Type().Elem()); e != nil {
		return e
	}
	if e := doc.Content[0].Decode(destination); e != nil {
		return fmt.Errorf("%w: typed YAML decode", ErrInvalid)
	}
	return nil
}
func checkNode(n *yaml.Node, depth int, count *int) error {
	*count++
	if depth > 64 || *count > 100000 || n.Kind == yaml.AliasNode || n.Anchor != "" {
		return ErrInvalid
	}
	if n.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || k.Value == "<<" || seen[k.Value] {
				return ErrInvalid
			}
			seen[k.Value] = true
		}
	}
	for _, c := range n.Content {
		if e := checkNode(c, depth+1, count); e != nil {
			return e
		}
	}
	return nil
}

var yamlUnmarshaler = reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()
var textUnmarshaler = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

func checkType(n *yaml.Node, t reflect.Type) error {
	// yaml.v3 also honors a legacy UnmarshalYAML callback signature. Reject
	// both signatures, rather than allowing either to bypass node validation.
	for _, candidate := range []reflect.Type{t, reflect.PointerTo(t)} {
		if _, ok := candidate.MethodByName("UnmarshalYAML"); ok {
			return ErrInvalid
		}
		if _, ok := candidate.MethodByName("UnmarshalText"); ok {
			return ErrInvalid
		}
	}
	if t.Implements(yamlUnmarshaler) || t.Implements(textUnmarshaler) || (t.Kind() != reflect.Pointer && (reflect.PointerTo(t).Implements(yamlUnmarshaler) || reflect.PointerTo(t).Implements(textUnmarshaler))) {
		return ErrInvalid
	}
	if t.Kind() == reflect.Pointer {
		if n.Tag == "!!null" {
			return nil
		}
		return checkType(n, t.Elem())
	}
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return ErrInvalid
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			parts := strings.Split(f.Tag.Get("yaml"), ",")
			name := parts[0]
			if name == "-" {
				continue
			}
			for _, p := range parts[1:] {
				if p == "inline" {
					return ErrInvalid
				}
			}
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			if _, ok := fields[name]; ok {
				return ErrInvalid
			}
			fields[name] = f.Type
		}
		for i := 0; i < len(n.Content); i += 2 {
			ft, ok := fields[n.Content[i].Value]
			if !ok {
				return ErrInvalid
			}
			if e := checkType(n.Content[i+1], ft); e != nil {
				return e
			}
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode || t.Key().Kind() != reflect.String {
			return ErrInvalid
		}
		for i := 0; i < len(n.Content); i += 2 {
			if e := checkType(n.Content[i], t.Key()); e != nil {
				return e
			}
			if e := checkType(n.Content[i+1], t.Elem()); e != nil {
				return e
			}
		}
	case reflect.Slice, reflect.Array:
		if n.Kind != yaml.SequenceNode {
			return ErrInvalid
		}
		if t.Kind() == reflect.Array && len(n.Content) != t.Len() {
			return ErrInvalid
		}
		for _, c := range n.Content {
			if e := checkType(c, t.Elem()); e != nil {
				return e
			}
		}
	case reflect.String:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
			return ErrInvalid
		}
	case reflect.Bool:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" {
			return ErrInvalid
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!int" {
			return ErrInvalid
		}
	case reflect.Float32, reflect.Float64:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!float" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
