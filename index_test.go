package elasticsearch

import (
	"bytes"
	"encoding/json"
	"net/url"
	"testing"
)

func TestIndexRequest(t *testing.T) {
	r := IndexRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "123",
		Params: url.Values{"refresh": {"true"}},
		Source: map[string]interface{}{
			"name": "John",
			"age":  24,
		},
	}

	if got := r.Method(); got != "PUT" {
		t.Errorf("expected method to be PUT; got %q", got)
	}

	if got := r.Path(); got != "foo/bar/123" {
		t.Errorf("expected path to be foo/bar/123; got %q", got)
	}

	if expected, got := r.Params.Encode(), r.Values().Encode(); got != expected {
		t.Errorf("expected values to be %q; got %q", expected, got)
	}

	expected, _ := json.Marshal(r.Source)
	expected = append(expected, '\n')

	buf := new(bytes.Buffer)
	if err := r.Serialize(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize to produce %q; got %q", expected, got)
	}

	expected, _ = json.Marshal(map[string]interface{}{
		"index": map[string]string{
			"_index": "foo",
			"_type": "bar",
			"_id": "123",
			"refresh": "true",
		},
	})
	expected = append(expected, '\n')

	buf = new(bytes.Buffer)
	if err := r.SerializeBatchHeader(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize batch header to produce %q; got %q", expected, got)
	}
}

func TestCreateRequest(t *testing.T) {
	r := CreateRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "123",
		Params: url.Values{"refresh": {"true"}},
		Source: map[string]interface{}{
			"name": "John",
			"age":  24,
		},
	}

	if got := r.Method(); got != "PUT" {
		t.Errorf("expected method to be PUT; got %q", got)
	}

	if got := r.Path(); got != "foo/bar/123/_create" {
		t.Errorf("expected path to be foo/bar/123/_create; got %q", got)
	}

	if expected, got := r.Params.Encode(), r.Values().Encode(); got != expected {
		t.Errorf("expected values to be %q; got %q", expected, got)
	}

	expected, _ := json.Marshal(r.Source)
	expected = append(expected, '\n')

	buf := new(bytes.Buffer)
	if err := r.Serialize(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize to produce %q; got %q", expected, got)
	}

	expected, _ = json.Marshal(map[string]interface{}{
		"create": map[string]string{
			"_index": "foo",
			"_type": "bar",
			"_id": "123",
			"refresh": "true",
		},
	})
	expected = append(expected, '\n')

	buf = new(bytes.Buffer)
	if err := r.SerializeBatchHeader(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize batch header to produce %q; got %q", expected, got)
	}
}

func TestUpdateRequest(t *testing.T) {
	r := UpdateRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "123",
		Params: url.Values{"refresh": {"true"}},
		Source: map[string]interface{}{
			"name": "John",
			"age":  24,
		},
	}

	if got := r.Method(); got != "POST" {
		t.Errorf("expected method to be POST; got %q", got)
	}

	if got := r.Path(); got != "foo/bar/123/_update" {
		t.Errorf("expected path to be foo/bar/123/_update; got %q", got)
	}

	if expected, got := r.Params.Encode(), r.Values().Encode(); got != expected {
		t.Errorf("expected values to be %q; got %q", expected, got)
	}

	expected, _ := json.Marshal(r.Source)
	expected = append(expected, '\n')

	buf := new(bytes.Buffer)
	if err := r.Serialize(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize to produce %q; got %q", expected, got)
	}

	expected, _ = json.Marshal(map[string]interface{}{
		"update": map[string]string{
			"_index": "foo",
			"_type": "bar",
			"_id": "123",
			"refresh": "true",
		},
	})
	expected = append(expected, '\n')

	buf = new(bytes.Buffer)
	if err := r.SerializeBatchHeader(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize batch header to produce %q; got %q", expected, got)
	}
}

func TestDeleteRequest(t *testing.T) {
	r := DeleteRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "123",
		Params: url.Values{"refresh": {"true"}},
	}

	if got := r.Method(); got != "DELETE" {
		t.Errorf("expected method to be DELETE; got %q", got)
	}

	if got := r.Path(); got != "foo/bar/123" {
		t.Errorf("expected path to be foo/bar/123; got %q", got)
	}

	if expected, got := r.Params.Encode(), r.Values().Encode(); got != expected {
		t.Errorf("expected values to be %q; got %q", expected, got)
	}

	expected := []byte{}
	buf := new(bytes.Buffer)
	if err := r.Serialize(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize to produce %q; got %q", expected, got)
	}

	expected, _ = json.Marshal(map[string]interface{}{
		"delete": map[string]string{
			"_index": "foo",
			"_type": "bar",
			"_id": "123",
			"refresh": "true",
		},
	})
	expected = append(expected, '\n')

	buf = new(bytes.Buffer)
	if err := r.SerializeBatchHeader(buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.Bytes(); string(got) != string(expected) {
		t.Errorf("expected serialize batch header to produce %q; got %q", expected, got)
	}
}

func TestBulkIndexRequest(t *testing.T) {
	index := IndexRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "123",
		Params: url.Values{"refresh": {"true"}},
		Source: map[string]interface{}{
			"name": "John",
		},
	}

	delete := DeleteRequest{
		Index:  "foo",
		Type:   "bar",
		Id:     "321",
	}

	r := BulkIndexRequest{index, delete}

	if expected, got := "POST", r.Method(); got != expected {
		t.Errorf("expected method to be %q; got %q", expected, got)
	}

	if expected, got := "/_bulk", r.Path(); got != expected {
		t.Errorf("expected method to be %q; got %q", expected, got)
	}

	expected := new(bytes.Buffer)
	encoder := json.NewEncoder(expected)
	encoder.Encode(map[string]interface{}{
		"index": map[string]string {
			"_index": "foo",
			"_type": "bar",
			"_id": "123",
			"refresh": "true",
		},
	})
	encoder.Encode(map[string]interface{}{"name": "John"})
	encoder.Encode(map[string]interface{}{
		"delete": map[string]string {
			"_index": "foo",
			"_type": "bar",
			"_id": "321",
		},
	})

	got := new(bytes.Buffer)
	if err := r.Serialize(got); err != nil {
		t.Fatal(err)
	}

	if got.String() != expected.String() {
		t.Errorf("expected serialize to produce %q; got %q", got.String(), expected.String())
	}
}
