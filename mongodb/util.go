package mongodb

import (
	"fmt"
	"reflect"

	"github.com/kickstar-io/go-sdk/mongodb/utils"

	"github.com/jinzhu/inflection"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Coll return model's collection.
func Coll(m Model, opts ...*options.CollectionOptions) *Collection {
	if collGetter, ok := m.(CollectionGetter); ok {
		return collGetter.Collection()
	}
	return CollectionByName(CollName(m), opts...)
}

func CollRead(m Model, opts ...*options.CollectionOptions) *Collection {
	readPref, err := readpref.New(readpref.NearestMode)

	if err != nil {
		fmt.Println("Error: errInvalidReadPreference")
		return nil
	}

	colOption := options.CollectionOptions{
		ReadPreference: readPref,
	}

	opts = append(opts, &colOption)

	return Coll(m, opts...)
}

func CollWithMode(m Model, mode readpref.Mode) *Collection {
	if collGetter, ok := m.(CollectionGetter); ok {
		return collGetter.Collection()
	}
	return CollectionByNameWithMode(CollName(m), mode)
}

// CollName check if you provided collection name in your
// model, return it's name, otherwise guess model
// collection's name.
func CollName(m Model) string {
	if collNameGetter, ok := m.(CollectionNameGetter); ok {
		return collNameGetter.CollectionName()
	}
	name := reflect.TypeOf(m).Elem().Name()
	return inflection.Plural(utils.ToSnakeCase(name))
}

// UpsertTrueOption returns new instance of the
// UpdateOptions with upsert=true property.
func UpsertTrueOption() *options.UpdateOptions {
	upsert := true
	return &options.UpdateOptions{Upsert: &upsert}
}

func WriteConcernW0() *options.CollectionOptions {
	return WriteConcern(0)
}
func WriteConcernW1() *options.CollectionOptions {
	return WriteConcern(1)
}

func WriteConcern(w int) *options.CollectionOptions {
	return &options.CollectionOptions{
		WriteConcern: writeconcern.New(writeconcern.W(w)),
	}
}
