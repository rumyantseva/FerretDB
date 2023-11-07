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
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/FerretDB/FerretDB/integration/setup"
)

// Run against FerretDB (from integration directory):
// task test-integration-postgresql TEST_RUN="TestQueryLogicalAnd"
func TestQueryLogicalAnd(t *testing.T) {
	t.Parallel()

	ctx, collection := setup.Setup(t, Int32s)

	// Inserted data - Int32s (sorted by v):
	// {{"_id", "int32-min"},  {"v", int32(math.MinInt32)}}, // -2147483648
	// {{"_id", "int32-zero"}, {"v", int32(0)}},
	// {{"_id", "int32-1"},    {"v", int32(1)}},
	// {{"_id", "int32"},      {"v", int32(42)}},
	// {{"_id", "int32-max"},  {"v", int32(math.MaxInt32)}}, // 2147483647

	for name, tc := range map[string]struct {
		filter bson.D   // required, filter to be tested
		res    []bson.D // expected result
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
				{{"_id", "int32-1"}, {"v", int32(1)}},
			},
		},
	} {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			opts := options.Find().SetSort(bson.D{{"v", 1}})
			cursor, err := collection.Find(ctx, tc.filter, opts)
			require.NoError(t, err)

			res := FetchAll(t, ctx, cursor)
			require.NoError(t, err)
			AssertEqualDocumentsSlice(t, tc.res, res)
		})
	}
}

// Values stores _id and value pair.
type Values map[string]any

// Int32s contains int32 values for tests.
var Int32s Values = map[string]any{
	"int32-min":  int32(math.MinInt32),
	"int32-zero": int32(0),
	"int32-1":    int32(1),
	"int32":      int32(42),
	"int32-max":  int32(math.MaxInt32),
}

// Name implement Provider interface.
func (vs Values) Name() string {
	return "Int32s"
}

// Docs implement Provider interface.
func (vs Values) Docs() []bson.D {
	res := make([]bson.D, 0, len(vs))

	for id, v := range vs {
		res = append(res, bson.D{{"_id", id}, {"v", v}})
	}

	return res
}
