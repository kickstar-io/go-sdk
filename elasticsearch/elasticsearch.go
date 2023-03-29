package elasticsearch

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/kickstar-io/go-sdk/elasticsearch/utils"

	el "github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Username              string
	Password              string
	Host                  []string
	Nodename              string
	ClusterName           string
	TimeoutConnect        int
	MaxIdleConnsPerHost   int
	PoolSize              int
	RetryOnStatus         []int
	DisableRetry          bool
	EnableRetryOnTimeout  bool
	MaxRetries            int
	RetriesTimeout        int
	ResponseHeaderTimeout int
	Index                 string
}

type ElasticClient struct {
	user                  string
	password              string
	host                  []string
	nodename              string
	clustername           string
	timeoutconnect        int
	maxIdleConnsPerHost   int
	poolSize              int
	retryOnStatus         []int
	disableRetry          bool
	enableRetryOnTimeout  bool
	maxRetries            int
	retriesTimeout        int
	responseHeaderTimeout int
	Client                *el.Client
	Index                 string
}

type Response struct {
	StatusCode int
	Header     http.Header
	Body       map[string]interface{}
}

func (e *ElasticClient) CheckConnetion() error {
	if e.Client == nil {
		log.Error("can't get elastic client")
		return errors.New("can't get elastic client")
	}
	return nil
}
func NewElasticClient(config Config) (IElasticConnector, error) {
	elasticPool := &ElasticClient{
		host:                  config.Host,
		user:                  config.Username,
		password:              config.Password,
		nodename:              config.Nodename,
		clustername:           config.ClusterName,
		timeoutconnect:        config.TimeoutConnect,
		maxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		poolSize:              config.PoolSize,
		retryOnStatus:         config.RetryOnStatus,
		disableRetry:          config.DisableRetry,
		enableRetryOnTimeout:  config.EnableRetryOnTimeout,
		maxRetries:            config.MaxRetries,
		retriesTimeout:        config.RetriesTimeout,
		responseHeaderTimeout: config.ResponseHeaderTimeout,
	}
	err := elasticPool.GetConn()
	if err != nil {
		return elasticPool, err
	}
	return elasticPool, nil
}

func (e *ElasticClient) GetConn() error {
	client, err := el.NewClient(
		el.SetBasicAuth(e.user, e.password),
		el.SetURL(strings.Join(e.host[:], ",")),
		el.SetSniff(false),
		// el.SetMaxRetries(e.maxRetries),
		el.SetHealthcheckInterval(time.Duration(e.responseHeaderTimeout)*time.Second),
		el.SetHealthcheckTimeout(time.Duration(e.responseHeaderTimeout)*time.Second),
	)
	if err != nil {
		// Handle error
		log.Error("ElasticClient - GetConn => Error : ", err.Error())
		return err
	}
	e.Client = client
	return nil
}

func (e *ElasticClient) GetClient() *el.Client {
	return e.Client
}

// Ping check connection
func (e *ElasticClient) Ping() error {
	if err := e.CheckConnetion(); err != nil {
		return err
	}
	for _, hostUrl := range e.host {
		info, code, err := e.Client.Ping(hostUrl).Do(context.Background())
		if err != nil {
			log.Error("Error getting response: ", err)
			return err
		}
		if code != 200 {
			log.Error("Connect Elasticsearch fail, code : ", code)
			return err
		}
		log.Info("Elasticsearch returned with code ", code, " and version ", info.Version.Number, "\n")
	}
	return nil
}

// Create elasticsearch index
func (e *ElasticClient) CreateIndex(name string, jsonBody interface{}) (interface{}, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	isExisted, err := e.Client.IndexExists(name).Do(context.Background()) //Check existed index
	if err != nil {
		log.Error("ElasticClient - CreateIndex => Error : ", err.Error())
		return nil, err
	}
	if !isExisted { // Index not existed --> Create index
		response, err := e.Client.CreateIndex(name).BodyJson(jsonBody).Do(context.Background())
		if err != nil {
			log.Error("ElasticClient - CreateIndex => Failed : ", err.Error())
			return nil, err
		}
		log.Info("ElasticClient - CreateIndex => Created : ", name)
		return response, nil
	} else {
		log.Info("ElasticClient - CreateIndex => Existed : ", name)
		return nil, nil
	}
}

// Delete index by name
// Sample: result, err := IE.ESConnector.DeleteIndex("index_name_test",jsonBody) ; // jsonBody := map[string]interface{}{"acknowledged": true}
func (e *ElasticClient) DeleteIndex(indexName string, jsonBody interface{}) (interface{}, error) { // name: index
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	isExisted, err := e.Client.IndexExists(indexName).Do(context.Background()) //Check existed index
	if err != nil {
		log.Error("ElasticClient - DeleteIndex => Error : ", err.Error())
		return nil, err
	}
	if isExisted { // Index existed --> Delete index
		response, err := e.Client.DeleteIndex(indexName).Do(context.Background())
		if err != nil {
			log.Error("ElasticClient - DeleteIndex => Failed : ", err.Error())
			return nil, err
		}
		log.Info("ElasticClient - DeleteIndex => OK : ", indexName)
		return response, nil
	} else {
		log.Info("ElasticClient - DeleteIndex => Not Existed : ", indexName)
		return nil, errors.New("index not existed")
	}
}

// Update one document of index
// Sample: result, err := IE.ESConnector.UpdateIndexSettings("index_name_test",body) ; // search := map[string]interface{}{"company":"Ahamove"}
func (e *ElasticClient) UpdateIndexSettings(indexName string, jsonBody string) (interface{}, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	isExisted, err := e.Client.IndexExists(indexName).Do(context.Background()) //Check existed index
	if err != nil {
		log.Error("ElasticClient - UpdateIndexSettings => Error : ", err.Error())
		return nil, err
	}
	if isExisted { // Index existed --> Update settings
		response, err := e.Client.IndexPutSettings().Index(indexName).BodyString(jsonBody).Do(context.Background())
		if err != nil {
			log.Error("ElasticClient - UpdateIndexSettings => Failed : ", err.Error())
			return nil, err
		}
		log.Info("ElasticClient - UpdateIndexSettings => OK : ", indexName)
		return response, nil
	} else {
		log.Info("ElasticClient - UpdateIndexSettings => Not Existed : ", indexName)
		return nil, errors.New("index not existed")
	}
}

// Create one document into index
// Sample: result, err := IE.ESConnector.CreateOne("index_name_test",body) ; // search := map[string]interface{}{"company":"Ahamove"}
func (e *ElasticClient) CreateOne(indexName, id string, body interface{}) (*el.IndexResponse, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	res, err := e.Client.Index().Index(indexName).Id(id).BodyJson(body).Refresh("true").Do(context.Background()) // Create document
	if err != nil {                                                                                              // Create doc failed
		return res, err
	}
	return res, err
}

// Update one document by id
// Sample: result, err := IE.ESConnector.UpdateOne("index_name_test",id,body) ; // search := map[string]interface{}{"company":"Ahamove"}
func (e *ElasticClient) UpdateOne(indexName, id string, body interface{}) (*el.UpdateResponse, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	update, err := e.Client.Update().Index(indexName).Id(id).Doc(body).Do(context.Background())
	if err != nil { // Create doc failed
		return update, err
	}
	return update, err
}

// Update by query
// Same: err := IE.ESConnector.UpdateByQuery("index_name_test",query, "receiver._id='12345';readAt=0")
func (e *ElasticClient) UpdateByQuery(indexName, script string, query el.Query) error {
	if err := e.CheckConnetion(); err != nil {
		return err
	}
	_, err := e.Client.UpdateByQuery().Query(query).Index(indexName).Script(el.NewScriptInline(script)).Do(context.Background())
	if err != nil { // Update doc failed
		return err
	}
	return nil
}

// Find all document with MatchQuery
// Sample: result, err := IE.ESConnector.FindOneMatch("index_name_test",search) ; // search := map[string]interface{}{"company":"Ahamove"}
func (e *ElasticClient) FindAllMatch(indexName string, search map[string]interface{}) ([]*el.SearchHit, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	// create a bool query
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element.(string)))
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq)
	// call findAll function
	return e.findAll(indexName, searchSource)
}

// Find first match document with MatchQuery
// Sample: result, err := IE.ESConnector.FindOneMatch("index_name_test",search) ; // search := map[string]interface{}{"company":"Ahamove"}
func (e *ElasticClient) FindOneMatch(indexName string, search map[string]interface{}) (*el.SearchHit, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	// create a bool query
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element.(string)))
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq)
	// call findOne function
	return e.findOne(indexName, searchSource)
}

// Find documents with limit, ofset use MatchQuery
// Sample:
// from, size := 0, 100
// search := map[string]interface{}{"company":"Ahamove"}
// result, err := IE.ESConnector.FindOneMatch("index_name_test",search, from, size)
func (e *ElasticClient) FindMatchPaging(indexName string, search map[string]interface{}, from, size int) (DataPagingResponse, error) {
	if err := e.CheckConnetion(); err != nil {
		return DataPagingResponse{}, err
	}
	// create a bool query
	utils.ValidatePaging(&from, &size)
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element))
	}
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element.(string)))
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq).From(from).Size(size)
	// call findAll function
	return e.findPaging(indexName, searchSource, from, size)
}

// Find documents with range by condition
// Sample:
// from, size := 0, 100
// search := map[string]interface{}{"company":"Ahamove"}
// result, err := IE.ESConnector.FindOneMatch("index_name_test",search, from, size)
func (e *ElasticClient) FindMatchWithRange(indexName string, search map[string]interface{}, searchCondition ...RangeSearch) ([]*el.SearchHit, error) {
	if err := e.CheckConnetion(); err != nil {
		return nil, err
	}
	// create a bool query
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element))
	}
	for _, element := range searchCondition {
		// append match query for search
		elQuery := el.NewRangeQuery(element.Key)
		if element.From != nil {
			elQuery.From(element.From)
		}
		if element.To != nil {
			elQuery.From(element.To)
		}
		bq = bq.Must(elQuery)
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq)
	// call findAll function
	return e.findAll(indexName, searchSource)
}

// Find documents with rage, limit, ofset use MatchQuery
// Sample:
// from, size := 0, 100
// search := map[string]interface{}{"company":"Ahamove"}
// result, err := IE.ESConnector.FindOneMatch("index_name_test",search, from, size)
func (e *ElasticClient) FindMatchWithRangePaging(indexName string, search map[string]interface{}, from, size int, searchCondition ...RangeSearch) (DataPagingResponse, error) {
	if err := e.CheckConnetion(); err != nil {
		return DataPagingResponse{}, err
	}
	// create a bool query
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element))
	}
	for _, element := range searchCondition {
		// append match query for search
		elQuery := el.NewRangeQuery(element.Key)
		if element.From != nil {
			elQuery.From(element.From)
		}
		if element.To != nil {
			elQuery.From(element.To)
		}
		bq = bq.Must(elQuery)
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq).From(from).Size(size)
	// call findPaging function
	return e.findPaging(indexName, searchSource, from, size)
}

// Count documents match conditions
// Sample:
// search := map[string]interface{}{"company":"Ahamove"}
// result, err := IE.ESConnector.CountMatchDocuments("index_name_test",search)
func (e *ElasticClient) CountMatchDocuments(indexName string, search map[string]interface{}) (int64, error) {
	// create a bool query
	bq := el.NewBoolQuery()
	for key, element := range search {
		// append match query for search
		bq = bq.Must(el.NewMatchQuery(key, element))
	}
	// create a searchSource with created bool query
	searchSource := el.NewSearchSource().Query(bq)
	// call count function
	return e.count(indexName, searchSource)
}
func (e *ElasticClient) CountSearchSource(indexName string, searchSource *el.SearchSource) (int64, error) {
	if err := e.CheckConnetion(); err != nil {
		return 0, err
	}
	// call count function
	return e.count(indexName, searchSource)
}

// Find SearchSource with paging
func (e *ElasticClient) FindSearchSourcePaging(indexName string, searchSource *el.SearchSource, from, size int) (DataPagingResponse, error) {
	if err := e.CheckConnetion(); err != nil {
		return DataPagingResponse{}, err
	}
	// create a bool query
	utils.ValidatePaging(&from, &size)
	// call findPaging function
	return e.findPaging(indexName, searchSource, from, size)
}
