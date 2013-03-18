package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
)

// Fireable defines anything which can be fired against the search cluster.
//
// A Fireable will always be transformed into an HTTP request where:
//
//		- the request method is Method()
//		- the request uri's path is Path()
//		- the request uri's parameters are Values()
//		- the request's body is written with Serialize()
//
type Fireable interface {
	Method() string
	Path() string
	Serialize(w io.Writer) error
	Values() url.Values
}

// BatchFireable defines a batch operation against the search cluster, namely,
// a multi search or bulk index operation.
type BatchFireable interface {
	Fireable
	SerializeBatchHeader(w io.Writer) error
}

//
//
//

type SearchRequest struct {
	Indices []string
	Types   []string
	Query   SubQuery
	Params  url.Values // can be nil unless explicitly set
}

func (r SearchRequest) Method() string {
	return "GET"
}

func (r SearchRequest) Path() string {
	switch true {
	case len(r.Indices) == 0 && len(r.Types) == 0:
		return fmt.Sprintf(
			"/_search", // all indices, all types
		)

	case len(r.Indices) > 0 && len(r.Types) == 0:
		return fmt.Sprintf(
			"/%s/_search",
			strings.Join(r.Indices, ","),
		)

	case len(r.Indices) == 0 && len(r.Types) > 0:
		return fmt.Sprintf(
			"/_all/%s/_search",
			strings.Join(r.Types, ","),
		)

	case len(r.Indices) > 0 && len(r.Types) > 0:
		return fmt.Sprintf(
			"/%s/%s/_search",
			strings.Join(r.Indices, ","),
			strings.Join(r.Types, ","),
		)
	}
	panic("unreachable")
}

func (r SearchRequest) Values() url.Values {
	if r.Params == nil {
		return url.Values{}
	}
	return r.Params
}

func (r SearchRequest) Serialize(w io.Writer) error {
	return json.NewEncoder(w).Encode(r.Query)
}

func (r SearchRequest) SerializeBatchHeader(w io.Writer) error {
	type headerLine struct {
		Indices []string `json:"index,omitempty"`
		Types   []string `json:"type,omitempty"`
	}

	return json.NewEncoder(w).Encode(headerLine{
		Indices: r.Indices,
		Types:   r.Types,
	})
}

//
//
//

type MultiSearchRequest []BatchFireable

func (r MultiSearchRequest) Method() string {
	return "GET"
}

func (r MultiSearchRequest) Path() string {
	return "/_msearch"
}

func (r MultiSearchRequest) Values() url.Values {
	v := url.Values{}
	for _, searchRequest := range r {
		for key, values := range searchRequest.Values() {
			for _, value := range values {
				v.Add(key, value)
			}
		}
	}
	return v
}

func (r MultiSearchRequest) Serialize(w io.Writer) error {
	for _, searchRequest := range r {
		if err := searchRequest.SerializeBatchHeader(w); err != nil {
			log.Printf("ElasticSearch: MultiSearchRequest Body header: %s", err)
			continue
		}

		if err := searchRequest.Serialize(w); err != nil {
			log.Printf("ElasticSearch: MultiSearchRequest Body body: %s", err)
			continue
		}
	}

	return nil
}
