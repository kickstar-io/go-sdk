package elasticsearch

import (
	el "github.com/olivere/elastic/v7"
)

// IElasticSearch - InterfaceP{}
type IElasticConnector interface {
	GetConn() error
	GetClient() *el.Client
	Ping() error
	// Index operations
	CreateIndex(indexName string, setting interface{}) (interface{}, error)    // Create index with index name and settings
	DeleteIndex(indexName string, setting interface{}) (interface{}, error)    // Delete index with index name and settings
	UpdateIndexSettings(indexName string, setting string) (interface{}, error) // Update index with index name and settings
	// Document operations
	CreateOne(index, id string, body interface{}) (*el.IndexResponse, error)  // Create document with index name, document id and body data
	UpdateOne(index, id string, body interface{}) (*el.UpdateResponse, error) // Update document with index name, document id and body data
	UpdateByQuery(indexName, script string, query el.Query) error             // Update document by query
	// Search documents operations
	// 1. Match search
	FindOneMatch(index string, search map[string]interface{}) (*el.SearchHit, error)                                                                      // Search one document of an index, return one hit | error
	FindAllMatch(index string, search map[string]interface{}) ([]*el.SearchHit, error)                                                                    // Search all document of an index, return list hits | error
	FindMatchPaging(indexName string, search map[string]interface{}, from, size int) (DataPagingResponse, error)                                          // Search documents with offset(from), limit(size), return list hits, error
	FindMatchWithRange(indexName string, search map[string]interface{}, searchCondition ...RangeSearch) ([]*el.SearchHit, error)                          // Search documents with range, return list hits, error
	FindMatchWithRangePaging(indexName string, search map[string]interface{}, from, size int, searchCondition ...RangeSearch) (DataPagingResponse, error) // Search documents with range and paging response, return list hits, error
	CountMatchDocuments(indexName string, search map[string]interface{}) (int64, error)                                                                   // count document match conditions
	FindSearchSourcePaging(indexName string, searchSource *el.SearchSource, from, size int) (DataPagingResponse, error)
	CountSearchSource(indexName string, searchSource *el.SearchSource) (int64, error)
}

var ESConnector IElasticConnector
var Index string
