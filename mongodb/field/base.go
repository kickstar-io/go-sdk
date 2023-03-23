package field

import "go.mongodb.org/mongo-driver/bson"

// ID field is just simple variable to predefine "_id" field.
const ID = "_id"

// Empty is predefined empty map.
var Empty = bson.M{}
