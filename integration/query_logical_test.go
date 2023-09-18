// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/FerretDB/FerretDB/integration/setup"
	"github.com/FerretDB/FerretDB/integration/shareddata"
)

// Run against MongoDB (from integration directory):
// go test -count=1 -run='TestQueryLogicalAnd' -target-url=mongodb://127.0.0.1:47017/ -target-backend=mongodb .
//
// Run against FerretDB:
// go test -count=1 -run='TestQueryLogicalAnd' -target-backend=ferretdb-pg -target-tls -postgresql-url=postgres://username@127.0.0.1:5432/ferretdb .
func TestQueryLogicalAnd(t *testing.T) {
	t.Parallel()

	ctx, collection := setup.Setup(t, shareddata.Int32s)

	// Inserted data - shareddata.Int32s (sorted by v):
	// {_id: "int32-min",  v: -2147483648}, // math.MinInt32
	// {_id: "int32-zero", v: 0},
	// {_id: "int32-1",    v: 1},
	// {_id: "int32",      v: 42},
	// {_id: "int32-max",  v: 2147483647}, // math.MaxInt32

	for name, tc := range map[string]struct {
		filter bson.D             // required, filter to be tested
		res    []bson.D           // expected result
		err    mongo.CommandError // expected error
	}{
		"Two": {
			// {$and: [{v: {$gt: 0}}, {v: {$lt: 42}}]}
			filter: bson.D{{
				"$and", bson.A{
					bson.D{{"v", bson.D{{"$gt", int32(0)}}}},
					bson.D{{"v", bson.D{{"$lt", int64(42)}}}},
				},
			}},
			res: []bson.D{
				{
					{"_id", "int32-1"},
					{"v", int32(1)},
				},
			},
		},
		"One": {
			// {$and: [{v: {$gt: 0}}]}
			filter: bson.D{{
				"$and", bson.A{
					bson.D{{"v", bson.D{{"$gt", int32(0)}}}},
				},
			}},
			res: []bson.D{
				{
					{"_id", "int32-1"},
					{"v", int32(1)},
				},
				{
					{"_id", "int32"},
					{"v", int32(42)},
				},
				{
					{"_id", "int32-max"},
					{"v", int32(math.MaxInt32)},
				},
			},
		},
		"AndAnd": {
			// {$and: [{$and: [{v: {$gt: 0}}, {v: {$lt: 42}}]}, {v: {$type: "int"}}]}
			filter: bson.D{{
				"$and", bson.A{
					bson.D{{"$and", bson.A{
						bson.D{{"v", bson.D{{"$gt", int32(0)}}}},
						bson.D{{"v", bson.D{{"$lte", 42.13}}}},
					}}},
					bson.D{{"v", bson.D{{"$type", "int"}}}},
				},
			}},
			res: []bson.D{
				{
					{"_id", "int32-1"},
					{"v", int32(1)},
				},
				{
					{"_id", "int32"},
					{"v", int32(42)},
				},
			},
		},
		"Zero": {
			// {$and: []}
			filter: bson.D{{
				"$and", bson.A{},
			}},
			err: mongo.CommandError{
				Code:    2,
				Name:    "BadValue",
				Message: "$and/$or/$nor must be a nonempty array",
			},
		},
		"BadInput": {
			filter: bson.D{{"$and", nil}},
			err: mongo.CommandError{
				Code:    2,
				Name:    "BadValue",
				Message: "$and must be an array",
			},
		},
	} {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			opts := options.Find().SetSort(bson.D{{"v", 1}})

			cursor, err := collection.Find(ctx, tc.filter, opts)

			if tc.res == nil {
				AssertEqualCommandError(t, tc.err, err)
				return
			}

			res := FetchAll(t, ctx, cursor)

			require.NoError(t, err)
			AssertEqualDocumentsSlice(t, tc.res, res)
		})
	}
}
