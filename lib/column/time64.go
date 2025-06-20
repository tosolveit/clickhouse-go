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

package column

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

type Time64 struct {
	chType   Type
	timezone *time.Location
	name     string
	col      proto.ColDateTime64
}

func (col *Time64) parse(t Type, tz *time.Location) (_ Interface, err error) {
	col.chType = t

	switch params := strings.Split(t.params(), ","); len(params) {
	case 2:
		precision, err := strconv.ParseInt(params[0], 10, 8)
		if err != nil {
			return nil, err
		}
		p := byte(precision)
		col.col.WithPrecision(proto.Precision(p))

		timezone, err := timezone.Load(params[1][2 : len(params[1])-1])
		if err != nil {
			return nil, err
		}
		col.col.WithLocation(timezone)

	case 1:
		precision, err := strconv.ParseInt(params[0], 10, 8)
		if err != nil {
			return nil, err
		}
		p := byte(precision)
		col.col.WithPrecision(proto.Precision(p))
		col.col.WithLocation(tz)

	default:
		return nil, &UnsupportedColumnTypeError{
			t: t,
		}
	}

	return col, nil
}

func (col *Time64) Name() string {
	return col.name
}

func (col *Time64) Type() Type {
	return col.chType
}

func (col *Time64) Rows() int {
	return col.col.Rows()
}

func (col *Time64) Row(i int, ptr bool) any {
	t := col.row(i)
	if ptr {
		return &t
	}
	return t
}

func (col *Time64) ScanRow(dest any, row int) error {
	t := col.row(row)
	switch d := dest.(type) {
	case *time.Time:
		*d = t
	case **time.Time:
		*d = new(time.Time)
		**d = t
	case *int64:
		*d = col.timeToInt64(t)
	case **int64:
		*d = new(int64)
		**d = col.timeToInt64(t)
	case *sql.NullTime:
		return d.Scan(t)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(t)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Time64",
		}
	}
	return nil
}

func (col *Time64) Append(v any) ([]uint8, error) {
	switch v := v.(type) {
	case []time.Time:
		nulls := make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
		return nulls, nil
	case []*time.Time:
		nulls := make([]uint8, len(v))
		for i := range v {
			if v[i] != nil {
				col.col.Append(*v[i])
			} else {
				col.col.Append(time.Time{})
				nulls[i] = 1
			}
		}
		return nulls, nil
	case []int64:
		nulls := make([]uint8, len(v))
		for i := range v {
			col.col.Append(time.UnixMilli(v[i]))
		}
		return nulls, nil
	case []*int64:
		nulls := make([]uint8, len(v))
		for i := range v {
			if v[i] != nil {
				col.col.Append(time.UnixMilli(*v[i]))
			} else {
				col.col.Append(time.Time{})
				nulls[i] = 1
			}
		}
		return nulls, nil
	case []sql.NullTime:
		nulls := make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
		return nulls, nil
	case []*sql.NullTime:
		nulls := make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
		return nulls, nil
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, err
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Time64",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *Time64) AppendRow(v any) error {
	switch v := v.(type) {
	case time.Time:
		col.col.Append(v)
	case *time.Time:
		if v != nil {
			col.col.Append(*v)
		} else {
			col.col.Append(time.Time{})
		}
	case int64:
		col.col.Append(time.UnixMilli(v))
	case *int64:
		if v != nil {
			col.col.Append(time.UnixMilli(*v))
		} else {
			col.col.Append(time.Time{})
		}
	case sql.NullTime:
		if v.Valid {
			col.col.Append(v.Time)
		} else {
			col.col.Append(time.Time{})
		}
	case *sql.NullTime:
		if v != nil && v.Valid {
			col.col.Append(v.Time)
		} else {
			col.col.Append(time.Time{})
		}
	case string:
		t, err := col.parseTime(v)
		if err != nil {
			return err
		}
		col.col.Append(t)
	case nil:
		col.col.Append(time.Time{})
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return err
			}
			return col.AppendRow(val)
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Time64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Time64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Time64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Time64) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Time64) Reset() {
	col.col.Reset()
}

func (col *Time64) row(i int) time.Time {
	t := col.col.Row(i)
	if col.timezone != nil {
		t = t.In(col.timezone)
	}
	return t
}

func (col *Time64) timeToInt64(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(math.Pow10(9-int(col.col.Precision)))
}

func (col *Time64) parseTime(value string) (time.Time, error) {
	const layout = "15:04:05.999999999"
	return time.Parse(layout, value)
}
