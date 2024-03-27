package base

import (
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"strings"
)

type CanalEvent struct {
	EventType  string         `json:"eventType"`
	SchemaName string         `json:"schemaName"`
	TableName  string         `json:"tableName"`
	Timestamp  uint32         `json:"timestamp"`
	Position   string         `json:"position"`
	RowBefore  map[string]any `json:"rowBefore,omitempty"`
	RowAfter   map[string]any `json:"rowAfter,omitempty"`
}

func GenCanalEventForEvent(timestamp uint32, tbInfo *TblInfoJson, rEv *replication.RowsEvent, sqlType, posStr string) []string {
	canalEvents := make([]string, 0)
	indexStep := 1
	if sqlType == "update" {
		indexStep = 2
	}

	for i := 0; i < len(rEv.Rows); i += indexStep {
		canalEvent := CanalEvent{
			EventType:  strings.ToUpper(sqlType),
			SchemaName: tbInfo.Database,
			TableName:  tbInfo.Table,
			Timestamp:  timestamp,
			Position:   posStr,
		}

		if sqlType == "insert" {
			canalEvent.RowAfter = genRowMap(tbInfo, rEv.Rows[i])
		} else if sqlType == "update" {
			canalEvent.RowBefore = genRowMap(tbInfo, rEv.Rows[i])
			canalEvent.RowAfter = genRowMap(tbInfo, rEv.Rows[i+1])
		} else if sqlType == "delete" {
			canalEvent.RowBefore = genRowMap(tbInfo, rEv.Rows[i])
		}

		data, err := json.Marshal(canalEvent)
		if err != nil {
			log.Errorf("canalEvent can not be marshaled, value: %v", canalEvent)
			continue
		}
		canalEvents = append(canalEvents, string(data))
	}
	return canalEvents
}

func genRowMap(tbInfo *TblInfoJson, rowImages []interface{}) map[string]any {
	if len(tbInfo.Columns) != len(rowImages) {
		log.Warnf("columns len:[%d] and value len:[%d] is not equal", len(tbInfo.Columns), len(rowImages))
		return nil
	}

	rowMap := make(map[string]any)
	for i, rowImage := range rowImages {
		rowMap[tbInfo.Columns[i].FieldName] = rowImage
	}
	return rowMap
}
