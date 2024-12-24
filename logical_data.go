package main

type RowData struct {
	LogPos    uint64 `json:"log_pos"` // LSN
	Action    string `json:"action"`
	Namespace string `json:"namespace"` // table  namespace
	Table     string `json:"table"`     // table  table
	Gtid      string `json:"gtid"`      // xid
	Timestamp uint32 `json:"timestamp"`
	Schema    string `json:"schema"` // db
	// insert / delete
	Values map[string]interface{} `json:"values,omitempty"`
	// before values update
	BeforeValues map[string]interface{} `json:"before_values,omitempty"`
	// after vaulues update
	AfterValues map[string]interface{} `json:"after_values,omitempty"`
}
