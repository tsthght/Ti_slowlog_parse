package slowlog_parse

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"github.com/Ti_slowlog_parse/hack"
	"unsafe"
)

const (
	MaxOfMaxAllowedPacket uint64 = 1073741824
)

const (
	// SlowLogRowPrefixStr is slow log row prefix.
	SlowLogRowPrefixStr = "# "
	// SlowLogSpaceMarkStr is slow log space mark.
	SlowLogSpaceMarkStr = ": "
	// SlowLogSQLSuffixStr is slow log suffix.
	SlowLogSQLSuffixStr = ";"
	// SlowLogTimeStr is slow log field name.
	SlowLogTimeStr = "Time"
	// SlowLogStartPrefixStr is slow log start row prefix.
	SlowLogStartPrefixStr = SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr
	// SlowLogTxnStartTSStr is slow log field name.
	SlowLogTxnStartTSStr = "Txn_start_ts"
	// SlowLogUserStr is slow log field name.
	SlowLogUserStr = "User"
	// SlowLogConnIDStr is slow log field name.
	SlowLogConnIDStr = "Conn_ID"
	// SlowLogQueryTimeStr is slow log field name.
	SlowLogQueryTimeStr = "Query_time"
	// SlowLogDBStr is slow log field name.
	SlowLogDBStr = "DB"
	// SlowLogIsInternalStr is slow log field name.
	SlowLogIsInternalStr = "Is_internal"
	// SlowLogIndexIDsStr is slow log field name.
	SlowLogIndexIDsStr = "Index_ids"
	// SlowLogDigestStr is slow log field name.
	SlowLogDigestStr = "Digest"
	// SlowLogQuerySQLStr is slow log field name.
	SlowLogQuerySQLStr = "Query" // use for slow log table, slow log will not print this field name but print sql directly.
	// SlowLogStatsInfoStr is plan stats info.
	SlowLogStatsInfoStr = "Stats"
	// SlowLogNumCopTasksStr is the number of cop-tasks.
	SlowLogNumCopTasksStr = "Num_cop_tasks"
	// SlowLogCopProcAvg is the average process time of all cop-tasks.
	SlowLogCopProcAvg = "Cop_proc_avg"
	// SlowLogCopProcP90 is the p90 process time of all cop-tasks.
	SlowLogCopProcP90 = "Cop_proc_p90"
	// SlowLogCopProcMax is the max process time of all cop-tasks.
	SlowLogCopProcMax = "Cop_proc_max"
	// SlowLogCopProcAddr is the address of TiKV where the cop-task which cost max process time run.
	SlowLogCopProcAddr = "Cop_proc_addr"
	// SlowLogCopWaitAvg is the average wait time of all cop-tasks.
	SlowLogCopWaitAvg = "Cop_wait_avg"
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90"
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of TiKV where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"

	// ProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	ProcessTimeStr = "Process_time"
	// WaitTimeStr means the time of all coprocessor wait.
	WaitTimeStr = "Wait_time"
	// BackoffTimeStr means the time of all back-off.
	BackoffTimeStr = "Backoff_time"
	// RequestCountStr means the request count.
	RequestCountStr = "Request_count"
	// TotalKeysStr means the total scan keys.
	TotalKeysStr = "Total_keys"
	// ProcessKeysStr means the total processed keys.
	ProcessKeysStr = "Processed_keys"
)

const (
	// SlowLogTimeFormat is the time format for slow log.
	SlowLogTimeFormat = time.RFC3339Nano
	// OldSlowLogTimeFormat is the first version of the the time format for slow log, This is use for compatibility.
	OldSlowLogTimeFormat = "2006-01-02-15:04:05.999999999 -0700"
)

type slowQueryTuple struct {
	time              time.Time
	txnStartTs        uint64
	user              string
	connID            uint64
	queryTime         float64
	processTime       float64
	waitTime          float64
	backOffTime       float64
	requestCount      uint64
	totalKeys         uint64
	processKeys       uint64
	db                string
	indexIDs          string
	isInternal        bool
	digest            string
	statsInfo         string
	avgProcessTime    float64
	p90ProcessTime    float64
	maxProcessTime    float64
	maxProcessAddress string
	avgWaitTime       float64
	p90WaitTime       float64
	maxWaitTime       float64
	maxWaitAddress    string
	memMax            int64
	sql               string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string) error {
	switch field {
	case SlowLogTimeStr:
		t, err := ParseTime(value)
		if err != nil {
			return errors.WithStack(err)
		}
		if t.Location() != tz {
			t = t.In(tz)
		}
		st.time = t
	case SlowLogTxnStartTSStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.txnStartTs = num
	case SlowLogUserStr:
		st.user = value
	case SlowLogConnIDStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.connID = num
	case SlowLogQueryTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.queryTime = num
	case ProcessTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.processTime = num
	case WaitTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.waitTime = num
	case BackoffTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.backOffTime = num
	case RequestCountStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.requestCount = num
	case TotalKeysStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.totalKeys = num
	case ProcessKeysStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.processKeys = num
	case SlowLogDBStr:
		st.db = value
	case SlowLogIndexIDsStr:
		st.indexIDs = value
	case SlowLogIsInternalStr:
		st.isInternal = value == "true"
	case SlowLogDigestStr:
		st.digest = value
	case SlowLogStatsInfoStr:
		st.statsInfo = value
	case SlowLogCopProcAvg:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.avgProcessTime = num
	case SlowLogCopProcP90:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.p90ProcessTime = num
	case SlowLogCopProcMax:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.maxProcessTime = num
	case SlowLogCopProcAddr:
		st.maxProcessAddress = value
	case SlowLogCopWaitAvg:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.avgWaitTime = num
	case SlowLogCopWaitP90:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.p90WaitTime = num
	case SlowLogCopWaitMax:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.maxWaitTime = num
	case SlowLogCopWaitAddr:
		st.maxWaitAddress = value
	case SlowLogMemMax:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.WithStack(err)
		}
		st.memMax = num
	case SlowLogQuerySQLStr:
		st.sql = value
	}
	return nil
}

func copySlowQueryTuple(ptr *slowQueryTuple) slowQueryTuple {
	return slowQueryTuple{
		ptr.time,
		ptr.txnStartTs,
		ptr.user,
		ptr.connID,
		ptr.queryTime,
		ptr.processTime,
		ptr.waitTime,
		ptr.backOffTime,
		ptr.requestCount,
		ptr.totalKeys,
		ptr.processKeys,
		ptr.db,
		ptr.indexIDs,
		ptr.isInternal,
		ptr.digest,
		ptr.statsInfo,
		ptr.avgProcessTime,
		ptr.p90ProcessTime,
		ptr.maxProcessTime,
		ptr.maxProcessAddress,
		ptr.avgWaitTime,
		ptr.p90WaitTime,
		ptr.maxWaitTime,
		ptr.maxWaitAddress,
		ptr.memMax,
		ptr.sql,
	}
}

func formatSlowQueryTuple(st slowQueryTuple) string {
	return fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v",
		st.time, st.txnStartTs,
		st.user, st.connID,
		st.queryTime, st.processTime, st.waitTime, st.backOffTime,
		st.requestCount, st.totalKeys, st.processKeys, st.db, st.indexIDs, st.isInternal,
		st.digest, st.statsInfo,
		st.avgProcessTime, st.p90ProcessTime, st.maxProcessTime, st.maxProcessAddress,
		st.avgWaitTime, st.p90WaitTime, st.maxWaitTime, st.maxWaitAddress,
		st.memMax,
		st.sql)
}

func parseSlowLogFile(tz *time.Location, filePath string) ([]slowQueryTuple, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = file.Close(); err != nil {
			//log
		}
	}()
	return parseSlowLog(tz, bufio.NewReader(file))
}

func parseSlowLog(tz *time.Location, reader *bufio.Reader) ([]slowQueryTuple, error) {
	startFlag := false
	var rows []slowQueryTuple
	var st *slowQueryTuple
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return rows, err
		}
		line := string(String(lineByte))
		if !startFlag && strings.HasPrefix(line, SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			err = st.setFieldValue(tz, SlowLogTimeStr, line[len(SlowLogStartPrefixStr):])
			if err != nil {
				return rows, err
			}
			startFlag = true
			continue
		}

		if startFlag {
			if strings.HasPrefix(line, SlowLogRowPrefixStr) {
				line = line[len(SlowLogRowPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					err = st.setFieldValue(tz, field, fieldValues[i+1])
					if err != nil {
						return rows, err
					}
				}
			} else if strings.HasSuffix(line, SlowLogSQLSuffixStr) {
				err = st.setFieldValue(tz, SlowLogQuerySQLStr, string(hack.Slice(line)))
				if err != nil {
					return rows, err
				}
				rows = append(rows, copySlowQueryTuple(st))
				startFlag = false
			}
		}
	}
}

func getOneLine(reader *bufio.Reader) ([]byte, error) {
	lineByte, isPrefix, err := reader.ReadLine()
	if err != nil {
		return lineByte, err
	}
	var tempLine []byte
	for isPrefix {
		tempLine, isPrefix, err = reader.ReadLine()
		lineByte = append(lineByte, tempLine ...)
		if len(lineByte) > int(MaxOfMaxAllowedPacket) {
			return lineByte, errors.Errorf("single line length exceeds limit: %v", MaxOfMaxAllowedPacket)
		}
		if err != nil {
			return lineByte, err
		}
	}
	return lineByte, err
}

func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(SlowLogTimeFormat, s)
	if err != nil {
		// This is for compatibility.
		t, err = time.Parse(OldSlowLogTimeFormat, s)
		if err != nil {
			err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, SlowLogTimeFormat, err)
		}
	}
	return t, err
}
