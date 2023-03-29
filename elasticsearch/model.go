package elasticsearch

// Range search model
type RangeSearch struct {
	Key  string
	From interface{}
	To   interface{}
}
type RangeType string

var (
	GREATER_THAN       RangeType = "gt"
	GREATER_THAN_EQUAL RangeType = "gte"
	LESS_THAN          RangeType = "lt"
	LESS_THAN_EQUAL    RangeType = "lte"
)
