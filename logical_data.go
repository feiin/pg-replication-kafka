package main

type RowData struct {
	LogPos    uint32 `json:"log_pos"` // LSN
	Action    string `json:"action"`
	Table     string `json:"table"` // table  public.table
	Gtid      string `json:"gtid"`  // xid
	Timestamp uint32 `json:"timestamp"`
	Schema    string `json:"schema"` // db
	// insert / delete
	Values map[string]interface{} `json:"values,omitempty"`
	// before values update
	BeforeValues map[string]interface{} `json:"before_values,omitempty"`
	// after vaulues update
	AfterValues map[string]interface{} `json:"after_values,omitempty"`
}
