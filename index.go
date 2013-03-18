package elasticsearch

import (
	"encoding/json"
	"io"
	"net/url"
	"path"
)

// Helper function for building the action_metadata header value for bulk index
// requests.
func actionMetadata(action, index, t, id string, v url.Values) map[string]interface{} {
	metadata := map[string]string{
		"_index": index,
		"_type":  t,
		"_id":    id,
	}

	for k := range v {
		metadata[k] = v.Get(k)
	}

	return map[string]interface{}{
		action: metadata,
	}
}

// Default indexing operation. Inserts or updates a document by index, type,
// and id.
type IndexRequest struct {
	Index  string
	Type   string
	Id     string
	Params url.Values
	Source interface{}
}

func (r IndexRequest) Method() string {
	return "PUT"
}

func (r IndexRequest) Path() string {
	return path.Join(r.Index, r.Type, r.Id)
}

func (r IndexRequest) Values() url.Values {
	return r.Params
}

func (r IndexRequest) Serialize(w io.Writer) error {
	return json.NewEncoder(w).Encode(r.Source)
}

func (r IndexRequest) SerializeBatchHeader(w io.Writer) error {
	return json.NewEncoder(w).Encode(actionMetadata(
		"index",
		r.Index,
		r.Type,
		r.Id,
		r.Params,
	))
}

// Inserts a document by index, type, and id. Returns an error if the document
// already exists.
type CreateRequest struct {
	Index  string
	Type   string
	Id     string
	Params url.Values
	Source interface{}
}

func (r CreateRequest) Method() string {
	return "PUT"
}

func (r CreateRequest) Path() string {
	return path.Join(r.Index, r.Type, r.Id, "_create")
}

func (r CreateRequest) Values() url.Values {
	return r.Params
}

func (r CreateRequest) Serialize(w io.Writer) error {
	return json.NewEncoder(w).Encode(r.Source)
}

func (r CreateRequest) SerializeBatchHeader(w io.Writer) error {
	return json.NewEncoder(w).Encode(actionMetadata(
		"create",
		r.Index,
		r.Type,
		r.Id,
		r.Params,
	))
}

// Partially updates a document by index, type, and id.
//
// See: http://www.elasticsearch.org/guide/reference/api/update.html
type UpdateRequest struct {
	Index  string
	Type   string
	Id     string
	Params url.Values
	Source interface{}
}

func (r UpdateRequest) Method() string {
	return "POST"
}

func (r UpdateRequest) Path() string {
	return path.Join(r.Index, r.Type, r.Id, "_update")
}

func (r UpdateRequest) Values() url.Values {
	return r.Params
}

func (r UpdateRequest) Serialize(w io.Writer) error {
	return json.NewEncoder(w).Encode(r.Source)
}

func (r UpdateRequest) SerializeBatchHeader(w io.Writer) error {
	return json.NewEncoder(w).Encode(actionMetadata(
		"update",
		r.Index,
		r.Type,
		r.Id,
		r.Params,
	))
}

// Deletes a document by index, type, and id.
type DeleteRequest struct {
	Index  string
	Type   string
	Id     string
	Params url.Values
}

func (r DeleteRequest) Method() string {
	return "DELETE"
}

func (r DeleteRequest) Path() string {
	return path.Join(r.Index, r.Type, r.Id)
}

func (r DeleteRequest) Values() url.Values {
	return r.Params
}

func (r DeleteRequest) Serialize(w io.Writer) error {
	return nil
}

func (r DeleteRequest) SerializeBatchHeader(w io.Writer) error {
	return json.NewEncoder(w).Encode(actionMetadata(
		"delete",
		r.Index,
		r.Type,
		r.Id,
		r.Params,
	))
}

// Allows documents to be indexed, created, updated, or deleted in batches.
type BulkIndexRequest []BatchFireable

func (br BulkIndexRequest) Method() string {
	return "POST"
}

func (br BulkIndexRequest) Path() string {
	return "/_bulk"
}

func (br BulkIndexRequest) Values() url.Values {
	return url.Values{}
}

func (br BulkIndexRequest) Serialize(w io.Writer) error {
	for _, request := range br {
		if err := request.SerializeBatchHeader(w); err != nil {
			return err
		}
		if err := request.Serialize(w); err != nil {
			return err
		}
	}

	return nil
}
