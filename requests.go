package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
)

// Fireable defines anything which can be fired against the search cluster.
// A Fireable will always be transformed into a HTTP GET against the given
// Path(), with the given Body().
type Fireable interface {
	Path() string
	Values() url.Values
	Body() ([]byte, error)
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

func (r SearchRequest) Body() ([]byte, error) {
	return json.Marshal(r.Query)
}

//
//
//

type MultiSearchRequest []SearchRequest

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

func (r MultiSearchRequest) Body() ([]byte, error) {
	type headerLine struct {
		Indices []string `json:"index,omitempty"`
		Types   []string `json:"type,omitempty"`
	}

	lines := [][]byte{}
	for _, searchRequest := range r {
		headerBuf, err := json.Marshal(headerLine{
			Indices: searchRequest.Indices,
			Types:   searchRequest.Types,
		})
		if err != nil {
			log.Printf("ElasticSearch: MultiSearchRequest Body header: %s", err)
			continue
		}

		bodyBuf, err := searchRequest.Body()
		if err != nil {
			log.Printf("ElasticSearch: MultiSearchRequest Body body: %s", err)
			continue
		}

		lines = append(lines, headerBuf)
		lines = append(lines, bodyBuf)
	}

	// need trailing '\n'
	// http://www.elasticsearch.org/guide/reference/api/multi-search.html
	return append(bytes.Join(lines, []byte{'\n'}), '\n'), nil
}
