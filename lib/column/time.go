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
	"reflect"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type Time struct {
	chType Type
	name   string
	col    proto.ColDateTime
}

func (col *Time) parse(t Type, tz *time.Location) (_ Interface, err error) {
	col.chType = t
	col.col.Location = tz // or UTC if you want
	return col, nil
}

func (col *Time) Name() string {
	return col.name
}

func (col *Time) Type() Type {
	return col.chType
}

func (col *Time) Rows() int {
	return col.col.Rows()
}
func (col *Time) Row(i int, ptr bool) any {
	t := col.row(i)
	if ptr {
		return &t
	}
	return t
}

func (col *Time) ScanRow(dest any, row int) error {
	t := col.row(row)
	switch d := dest.(type) {
	case *time.Time:
		*d = t
	case **time.Time:
		*d = new(time.Time)
		**d = t
	case *int64: // epoch seconds
		*d = t.Unix()
	case **int64:
		*d = new(int64)
		**d = t.Unix()
	case *sql.NullTime:
		return d.Scan(t)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(t)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Time",
		}
	}
	return nil
}

func (col *Time) Append(v any) ([]uint8, error) {
	switch v := v.(type) {
	case []time.Time:
		nulls := make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
		return nulls, nil
	// handle []*time.Time, []int64, []*int64, sql.NullTime, etc.
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
			To:   "Time",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *Time) AppendRow(v any) error {
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
		col.col.Append(time.Unix(v, 0))
	case *int64:
		if v != nil {
			col.col.Append(time.Unix(*v, 0))
		} else {
			col.col.Append(time.Time{})
		}
	case sql.NullTime:
		if v.Valid {
			col.col.Append(v.Time)
		} else {
			col.col.Append(time.Time{})
		}
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
			To:   "Time",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Time) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Time) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Time) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Time) Reset() {
	col.col.Reset()
}

func (col *Time) row(i int) time.Time {
	// Get stored time; it will include a dummy date
	return col.col.Row(i)
}
