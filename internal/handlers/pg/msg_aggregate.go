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

package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"

	"github.com/FerretDB/FerretDB/internal/handlers/common"
	"github.com/FerretDB/FerretDB/internal/handlers/common/aggregations"
	"github.com/FerretDB/FerretDB/internal/handlers/commonerrors"
	"github.com/FerretDB/FerretDB/internal/handlers/pg/pgdb"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/wire"
)

// MsgAggregate implements HandlerInterface.
func (h *Handler) MsgAggregate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	dbPool, err := h.DBPool(ctx)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	// TODO https://github.com/FerretDB/FerretDB/issues/1892
	common.Ignored(document, h.L, "cursor", "lsid")

	if err = common.Unimplemented(document, "explain", "bypassDocumentValidation", "hint"); err != nil {
		return nil, err
	}

	if err = common.Unimplemented(document, "readConcern", "writeConcern"); err != nil {
		return nil, err
	}

	if err = common.Unimplemented(document, "let"); err != nil {
		return nil, err
	}

	common.Ignored(document, h.L, "allowDiskUse", "maxTimeMS", "collation", "comment")

	var sp pgdb.SQLParam

	if sp.DB, err = common.GetRequiredParam[string](document, "$db"); err != nil {
		return nil, err
	}

	collection, err := document.Get(document.Command())
	if err != nil {
		return nil, err
	}

	// TODO handle collection-agnostic pipelines ({aggregate: 1})
	// https://github.com/FerretDB/FerretDB/issues/1890
	var ok bool
	if sp.Collection, ok = collection.(string); !ok {
		return nil, common.NewCommandErrorMsgWithArgument(
			commonerrors.ErrBadValue,
			fmt.Sprintf("collection name has invalid type %s", common.AliasFromType(collection)),
			document.Command(),
		)
	}

	pipeline, err := common.GetRequiredParam[*types.Array](document, "pipeline")
	if err != nil {
		return nil, err
	}

	stagesDocs := must.NotFail(iterator.Values(pipeline.Iterator()))
	stages := make([]aggregations.Stage, len(stagesDocs))

	for i, d := range stagesDocs {
		var s aggregations.Stage

		if s, err = aggregations.NewStage(d.(*types.Document)); err != nil {
			return nil, err
		}

		stages[i] = s
	}

	// TODO pushdown `$match` https://github.com/FerretDB/FerretDB/issues/1894

	var docs []*types.Document
	err = dbPool.InTransaction(ctx, func(tx pgx.Tx) error {
		iter, getErr := pgdb.GetDocuments(ctx, tx, &sp)
		if getErr != nil {
			return getErr
		}

		docs, err = iterator.Values(iter)
		return err
	})

	if err != nil {
		return nil, err
	}

	for _, s := range stages {
		if docs, err = s.Process(ctx, docs); err != nil {
			return nil, err
		}
	}

	// TODO https://github.com/FerretDB/FerretDB/issues/1892
	firstBatch := types.MakeArray(len(docs))
	for _, doc := range docs {
		firstBatch.Append(doc)
	}

	var reply wire.OpMsg
	must.NoError(reply.SetSections(wire.OpMsgSection{
		Documents: []*types.Document{must.NotFail(types.NewDocument(
			"cursor", must.NotFail(types.NewDocument(
				"firstBatch", firstBatch,
				"id", int64(0),
				"ns", sp.DB+"."+sp.Collection,
			)),
			"ok", float64(1),
		))},
	}))

	return &reply, nil
}
