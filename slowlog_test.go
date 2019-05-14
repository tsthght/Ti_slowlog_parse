package slowlog_parse

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestParseSlowLog(t *testing.T) {
	slowLog := bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# User: root@127.0.0.1
# Conn_ID: 6
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# DB: test
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
select * from t;`)
	//expectedResult := "2019-04-28 15:24:04.309074 +0800 CST, 405888132465033227, 0.216905, 0.021, 1, 637, 436, true, 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772, t1:1,t2:2, 0.1, 0.2, 0.03, 127.0.0.1:20160, 0.05, 0.6, 0.8, 0.0.0.0:20160, 70724, select * from t;"
	reader := bufio.NewReader(slowLog)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	rows, _ := parseSlowLog(loc, reader)
	for _, row := range rows {
		/*
		recordString := fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v",
			row.time, row.txnStartTs, row.queryTime,
			row.processTime, row.requestCount, row.totalKeys, row.processKeys, row.isInternal,
			row.digest,
			row.statsInfo,
			row.avgProcessTime, row.p90ProcessTime, row.maxProcessTime, row.maxProcessAddress,
			row.avgWaitTime, row.p90WaitTime, row.maxWaitTime, row.maxWaitAddress,
			row.memMax,
			row.sql)

		fmt.Printf("## %s", recordString)
		*/
		fmt.Printf("format: %s", FormatSlowQueryTuple(row))
	}

}
