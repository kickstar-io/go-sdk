package elasticsearch

import el "github.com/olivere/elastic/v7"

// Result response
type DataPagingResponse struct {
	Total  int64
	Limit  int
	Offset int
	Data   []*el.SearchHit
}
