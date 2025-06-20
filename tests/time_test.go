// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)

func TestTimeColumn(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE test_time (
				Col1 Time,
				Col2 Nullable(Time),
				Col3 Array(Time),
				Col4 Array(Nullable(Time))
			) Engine MergeTree() ORDER BY tuple()
		`

		defer conn.Exec(ctx, "DROP TABLE IF EXISTS test_time")
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time")
		require.NoError(t, err)

		// Example value: 13:45:30 as seconds since midnight
		now := time.Date(2023, 1, 1, 13, 45, 30, 0, time.UTC)

		require.NoError(t, batch.Append(
			now,
			&now,
			[]time.Time{now, now.Add(1 * time.Hour)},
			[]*time.Time{&now, nil},
		))
		require.NoError(t, batch.Send())

		var (
			col1 time.Time
			col2 *time.Time
			col3 []time.Time
			col4 []*time.Time
		)

		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time").Scan(
			&col1, &col2, &col3, &col4,
		))

		assert.Equal(t, now.Hour(), col1.Hour())
		assert.NotNil(t, col2)
		assert.Equal(t, now.Hour(), col2.Hour())
		require.Len(t, col3, 2)
		assert.Equal(t, now.Hour(), col3[0].Hour())
		assert.Equal(t, now.Add(1*time.Hour).Hour(), col3[1].Hour())
		require.Len(t, col4, 2)
		assert.NotNil(t, col4[0])
		assert.Nil(t, col4[1])
	})
}
