package elasticsearch

import (
	"context"
	"errors"

	el "github.com/olivere/elastic/v7"
)

// implement find one document with SearchSource
// Sample:
// searchSource := el.NewSearchSource().Query(el.NewMatchQuery("company", "Ahamove"))
// result, err := e.findOne("index_name_test",searchSource)
func (e *ElasticClient) findOne(index string, searchSource *el.SearchSource) (*el.SearchHit, error) {
	// call findAll function
	result := el.SearchHit{}
	// get 1 HitChild call findPaging
	res, err := e.findPaging(index, searchSource, 0, 1)
	if err != nil {
		return &result, err
	}
	// get first element
	if len(res.Data) > 0 {
		return res.Data[0], nil
	}
	// not found element
	return &result, errors.New("not found")
}

// implement find all document with SearchSource
// Sample:
// searchSource := el.NewSearchSource().Query(el.NewMatchQuery("company", "Ahamove"))
// result, err := e.findAll("index_name_test",searchSource)
func (e *ElasticClient) findAll(index string, searchSource *el.SearchSource) ([]*el.SearchHit, error) {
	res, err := e.Client.Search().Index(index).SearchSource(searchSource).Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res.Hits.Hits, nil
}

// implement find documents with paging use SearchSource
// Sample:
// searchSource := el.NewSearchSource().Query(el.NewMatchQuery("company", "Ahamove"))
// result, err := e.findPaging("index_name_test",searchSource,0,1)
func (e *ElasticClient) findPaging(index string, searchSource *el.SearchSource, from, size int) (DataPagingResponse, error) {
	result := DataPagingResponse{}
	// call search request
	res, err := e.Client.Search().Index(index).SearchSource(searchSource).Do(context.Background())
	if err != nil {
		return result, err
	}
	// prepare result response
	listHits := res.Hits.Hits
	result.Offset = from
	result.Limit = size
	result.Data = listHits
	result.Total = res.Hits.TotalHits.Value
	return result, nil
}

// implement count documents use SearchSource
// Sample:
// searchSource := el.NewSearchSource().Query(el.NewMatchQuery("company", "Ahamove"))
// result, err := e.count("index_name_test",searchSource)
func (e *ElasticClient) count(index string, searchSource *el.SearchSource) (int64, error) {
	// call search request
	res, err := e.Client.Search().Index(index).TrackTotalHits(false).SearchSource(searchSource).Do(context.Background())
	if err != nil {
		return 0, err
	}
	return res.Hits.TotalHits.Value, nil
}
