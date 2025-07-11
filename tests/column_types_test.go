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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypes(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		const query = `
		SELECT
			  CAST(1   AS UInt8)  AS Col1
			, CAST('X' AS String) AS Col2
	`

		require.NoError(t, err)
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		types := rows.ColumnTypes()
		require.Len(t, types, 2)
		for i, v := range types {
			switch i {
			case 0:
				require.False(t, v.Nullable())
				assert.Equal(t, "Col1", v.Name())
				assert.Equal(t, reflect.TypeOf(uint8(0)), v.ScanType())
				assert.Equal(t, "UInt8", v.DatabaseTypeName())

			case 1:
				require.False(t, v.Nullable())
				assert.Equal(t, "Col2", v.Name())
				assert.Equal(t, reflect.TypeOf(""), v.ScanType())
				assert.Equal(t, "String", v.DatabaseTypeName())
			}
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}
