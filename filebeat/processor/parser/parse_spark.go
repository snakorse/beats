package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/processors"
	"github.com/pkg/errors"
)

const (
	executionIDKey    = "executionId"
	sqlExecutionIDKey = "spark.sql.execution.id"
	jobIDKey          = "Job ID"
	stageInfoKey      = "Stage Info"
	stageIDKey        = "Stage ID"
	propertiesKey     = "Properties"
)

type parseSpark struct {
	sync.RWMutex
	path            string
	flushTimeout    time.Duration
	executionExpire time.Duration
	jobExpire       time.Duration
	stageExpire     time.Duration
	rel             *sparkRel
}

type sparkRel struct {
	ExecutionRel       map[int64]time.Time           `json:"execution_rel"`
	ExecutionJobsRel   map[int64]map[int64]time.Time `json:"execution_jobs_rel"`
	ExecutionStagesRel map[int64]map[int64]time.Time `json:"execution_stages_rel"`
	JobExecutionRel    map[int64]int64               `json:"job_execution_rel"`
	StageExecutionRel  map[int64]int64               `json:"stage_execution_rel"`
}

type parseSparkConfig struct {
	Path            string        `config:"path"`
	FlushTimeout    time.Duration `config:"flush_timeout"`
	ExecutionExpire time.Duration `config:"execution_expire"`
	JobExpire       time.Duration `config:"job_expire"`
	StageExpire     time.Duration `config:"stage_expire"`
}

var defaultParseSparkConfig = parseSparkConfig{
	FlushTimeout:    time.Minute,
	ExecutionExpire: time.Hour,
	JobExpire:       time.Hour,
	StageExpire:     time.Hour,
}

func init() {
	processors.RegisterPlugin("parse_spark", newParseSpark)
}

func newParseSpark(c *common.Config) (processors.Processor, error) {
	logp.Debug("parse_spark", "new parse spark processor")

	config := defaultParseSparkConfig
	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("fail to unpack parse spark config")
		return nil, fmt.Errorf("fail to unpack the parse spark config: %s", err)
	}

	p := &parseSpark{
		path:            config.Path,
		flushTimeout:    config.FlushTimeout,
		executionExpire: config.ExecutionExpire,
		jobExpire:       config.JobExpire,
		stageExpire:     config.StageExpire,
	}
	p.initPath()
	p.loadRel()
	p.fillRel()
	go p.flushRel()
	return p, nil
}

func (p *parseSpark) Run(event *beat.Event) (*beat.Event, error) {

	var spark map[string]interface{}
	if v, err := event.GetValue("spark"); err != nil {
		logp.Warn("fail to get spark value in event: %v", event)
		return event, err
	} else if m, ok := v.(map[string]interface{}); !ok {
		logp.Warn("fail to convert spark value: %v to map", v)
		return event, nil
	} else {
		spark = m
	}

	var sparkID string
	id, ok := p.parseSparkMap(spark)
	if !ok {
		var source string
		if v, err := event.GetValue("source"); err != nil {
			logp.Warn("fail to get source value in event: %v", event)
			return event, err
		} else if s, ok := v.(string); !ok {
			logp.Warn("fail to convert source value: %v to string", v)
			return event, nil
		} else {
			source = s
		}
		sparkID = p.parseSparkPath(source)
	} else {
		sparkID = strconv.FormatInt(id, 10)
	}

	logp.Debug("parse_spark", "fill event spark id: %s", sparkID)
	if sparkID == "" {
		return event, nil
	}
	event.PutValue("terminus.id", sparkID)
	event.PutValue("terminus.source", "spark")
	return event, nil
}

func (p *parseSpark) parseSparkPath(path string) string {
	parts := strings.Split(path, "/")
	name := parts[len(parts)-1]
	if !strings.HasPrefix(name, "spark-") {
		return ""
	}
	sparkID := strings.TrimPrefix(name, "spark-")
	if index := strings.Index(sparkID, "."); index >= 0 {
		sparkID = sparkID[0:index]
	}
	return sparkID
}

func (p *parseSpark) parseSparkMap(m map[string]interface{}) (int64, bool) {
	event, ok := getMapValueString(m, "Event")
	if !ok {
		return 0, false
	}

	switch event {
	// Spark 本身的事件
	case "SparkListenerLogStart",
		"SparkListenerBlockManagerAdded",
		"SparkListenerEnvironmentUpdate",
		"SparkListenerApplicationStart",
		"SparkListenerExecutorAdded",
		"SparkListenerApplicationEnd":
		return 0, false
		// 任务执行相关
	case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
		executionID, ok := getMapValueInt64(m, executionIDKey)
		if !ok {
			return 0, false
		}

		p.Lock()
		p.rel.ExecutionRel[executionID] = time.Now()
		p.rel.ExecutionJobsRel[executionID] = make(map[int64]time.Time)
		p.rel.ExecutionStagesRel[executionID] = make(map[int64]time.Time)
		p.Unlock()

		return executionID, true
	case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
		executionID, ok := getMapValueInt64(m, executionIDKey)
		if !ok {
			return 0, false
		}

		p.Lock()
		delete(p.rel.ExecutionRel, executionID)
		for k := range p.rel.ExecutionJobsRel[executionID] {
			delete(p.rel.JobExecutionRel, k)
		}
		for k := range p.rel.ExecutionStagesRel[executionID] {
			delete(p.rel.StageExecutionRel, k)
		}
		delete(p.rel.ExecutionJobsRel, executionID)
		delete(p.rel.ExecutionStagesRel, executionID)
		p.Unlock()

		return executionID, true
	case "SparkListenerJobStart":
		jobID, ok := getMapValueInt64(m, jobIDKey)
		if !ok {
			return 0, false
		}
		properties, ok := getMapValueMap(m, propertiesKey)
		if !ok {
			return 0, false
		}
		sqlExecutionID, ok := getMapValueString(properties, sqlExecutionIDKey)
		if !ok {
			return 0, false
		}
		executionID, err := strconv.ParseInt(sqlExecutionID, 10, 64)
		if err != nil {
			return 0, false
		}

		p.RLock()
		_, ok = p.rel.ExecutionRel[executionID]
		jobs := p.rel.ExecutionJobsRel[executionID]
		p.RUnlock()
		if !ok || jobs == nil {
			return 0, false
		}

		p.Lock()
		p.rel.ExecutionJobsRel[executionID][jobID] = time.Now()
		p.rel.JobExecutionRel[jobID] = executionID
		p.Unlock()

		return executionID, true
	case "SparkListenerJobEnd":
		jobID, ok := getMapValueInt64(m, jobIDKey)
		if !ok {
			return 0, false
		}
		p.RLock()
		executionID, ok := p.rel.JobExecutionRel[jobID]
		p.RUnlock()
		if !ok {
			return 0, false
		}

		p.Lock()
		delete(p.rel.JobExecutionRel, jobID)
		delete(p.rel.ExecutionJobsRel[executionID], jobID)
		p.Unlock()

		return executionID, true
	case "SparkListenerStageSubmitted":
		stage, ok := getMapValueMap(m, stageInfoKey)
		if !ok {
			return 0, false
		}
		stageID, ok := getMapValueInt64(stage, stageIDKey)
		if !ok {
			return 0, false
		}
		properties, ok := getMapValueMap(m, propertiesKey)
		if !ok {
			return 0, false
		}
		sqlExecutionID, ok := getMapValueString(properties, sqlExecutionIDKey)
		if !ok {
			return 0, false
		}
		executionID, err := strconv.ParseInt(sqlExecutionID, 10, 64)
		if err != nil {
			return 0, false
		}

		p.RLock()
		_, ok = p.rel.ExecutionRel[executionID]
		stages := p.rel.ExecutionStagesRel[executionID]
		p.RUnlock()
		if !ok || stages == nil {
			return 0, false
		}

		p.Lock()
		p.rel.ExecutionStagesRel[executionID][stageID] = time.Now()
		p.rel.StageExecutionRel[stageID] = executionID
		p.Unlock()

		return executionID, true
	case "SparkListenerStageCompleted":
		stage, ok := getMapValueMap(m, stageInfoKey)
		if !ok {
			return 0, false
		}
		stageID, ok := getMapValueInt64(stage, stageIDKey)
		if !ok {
			return 0, false
		}

		p.RLock()
		executionID, ok := p.rel.StageExecutionRel[stageID]
		p.RUnlock()
		if !ok {
			return 0, false
		}

		p.Lock()
		delete(p.rel.StageExecutionRel, stageID)
		delete(p.rel.ExecutionStagesRel[executionID], stageID)
		p.Unlock()

		return executionID, true
	case "SparkListenerTaskStart", "SparkListenerTaskEnd":
		stageID, ok := getMapValueInt64(m, stageIDKey)
		if !ok {
			return 0, false
		}

		p.RLock()
		executionID, ok := p.rel.StageExecutionRel[stageID]
		p.RUnlock()
		if !ok {
			return 0, false
		}

		return executionID, true
	default:
		return 0, false
	}
}

func (p *parseSpark) initPath() error {
	if p.path == "" {
		logp.Warn("spark rel path is empty")
		return errors.Errorf("spark rel path is empty")
	}
	dir := filepath.Dir(p.path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return errors.Errorf("fail to created spark rel file dir %s: %v", p.path, err)
	}

	if fileInfo, err := os.Lstat(p.path); os.IsNotExist(err) {
		logp.Info("fail to find spark rel file under: %s. create a new spark rel file.", p.path)
		return p.writeRel()
	} else if err != nil {
		return errors.Errorf("fail to get spark rel file %s info", p.path)
	} else if !fileInfo.Mode().IsRegular() {
		if fileInfo.IsDir() {
			return errors.Errorf("spark rel file path must be a file. %s is a directory.", p.path)
		}
		return errors.Errorf("spark rel file path is not a regular file: %s", p.path)
	}
	return nil
}

func (p *parseSpark) fillRel() error {
	if p.rel == nil {
		p.rel = new(sparkRel)
	}
	if p.rel.ExecutionRel == nil {
		p.rel.ExecutionRel = make(map[int64]time.Time)
	}
	if p.rel.ExecutionJobsRel == nil {
		p.rel.ExecutionJobsRel = make(map[int64]map[int64]time.Time)
	}
	if p.rel.ExecutionStagesRel == nil {
		p.rel.ExecutionStagesRel = make(map[int64]map[int64]time.Time)
	}
	if p.rel.JobExecutionRel == nil {
		p.rel.JobExecutionRel = make(map[int64]int64)
	}
	if p.rel.StageExecutionRel == nil {
		p.rel.StageExecutionRel = make(map[int64]int64)
	}
	return nil
}

func (p *parseSpark) loadRel() error {
	f, err := os.Open(p.path)
	if err != nil {
		return err
	}
	defer f.Close()
	logp.Info("load spark rel data from %s", p.path)

	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&p.rel); err != nil {
		return errors.Wrap(err, "fail to decode spark rel data")
	}
	return nil
}

func (p *parseSpark) flushRel() {
	logp.Debug("parse_spark", "flush spark rel to file: %v", p.flushTimeout)
	for {
		time.Sleep(p.flushTimeout)
		logp.Debug("parse_spark", "flush spark rel wake up")

		if p.rel == nil {
			continue
		}

		p.RLock()
		var expireExecutions, expireJobs, expireStages []int64
		executionTimeout := time.Now().Add(-p.executionExpire)
		for k, v := range p.rel.ExecutionRel {
			if v.Before(executionTimeout) {
				expireExecutions = append(expireExecutions, k)
			}
		}
		jobTimeout := time.Now().Add(-p.jobExpire)
		for _, jobs := range p.rel.ExecutionJobsRel {
			for k, v := range jobs {
				if v.Before(jobTimeout) {
					expireJobs = append(expireJobs, k)
				}
			}
		}
		stageTimeout := time.Now().Add(-p.stageExpire)
		for _, stages := range p.rel.ExecutionStagesRel {
			for k, v := range stages {
				if v.Before(stageTimeout) {
					expireStages = append(expireStages, k)
				}
			}
		}
		p.RUnlock()

		p.Lock()
		for _, k := range expireExecutions {
			delete(p.rel.ExecutionRel, k)
			delete(p.rel.ExecutionJobsRel, k)
			delete(p.rel.ExecutionStagesRel, k)
		}
		for _, k := range expireJobs {
			executionID, ok := p.rel.JobExecutionRel[k]
			if !ok {
				continue
			}
			delete(p.rel.ExecutionJobsRel[executionID], k)
		}
		for _, k := range expireStages {
			executionID, ok := p.rel.StageExecutionRel[k]
			if !ok {
				continue
			}
			delete(p.rel.ExecutionStagesRel[executionID], k)
		}
		p.Unlock()

		if err := p.writeRel(); err != nil {
			logp.Warn("fail to flush spark rel to write file: %s", err)
		}
	}
}

func (p *parseSpark) writeRel() error {
	logp.Debug("parse_spark", "write spark rel file: %s", p.path)

	f, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		logp.Err("fail to write spark rel file %s: %s", p.path, err)
		return err
	}
	defer f.Close()

	s, err := json.Marshal(p.rel)
	if err != nil {
		logp.Err("fail to marshal spark rel: %s", err)
		return err
	}
	logp.Debug("parse_spark", "spark rel: %s", string(s))

	if p.rel == nil {
		p.fillRel()
	}
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(p.rel); err != nil {
		logp.Err("fail to encode spark rel: %s", err)
		return err
	}

	if err = f.Sync(); err != nil {
		logp.Err("fail to sync spark rel to file %s: %s", p.path, err)
		return err
	}
	return nil
}

func getMapValueMap(m map[string]interface{}, key string) (map[string]interface{}, bool) {
	value, ok := m[key]
	if !ok {
		return nil, false
	}
	mv, ok := value.(map[string]interface{})
	return mv, ok
}

func GetMapValueArr(m map[string]interface{}, key string) ([]interface{}, bool) {
	value, ok := m[key]
	if !ok {
		return nil, false
	}
	arr, ok := value.([]interface{})
	return arr, ok
}

func getMapValueString(m map[string]interface{}, key string) (string, bool) {
	value, ok := m[key]
	if !ok {
		return "", false
	}
	return convertString(value)
}

func getMapValueBool(m map[string]interface{}, key string) (bool, bool) {
	value, ok := m[key]
	if !ok {
		return false, false
	}
	return convertBool(value)
}

func getMapValueInt64(m map[string]interface{}, key string) (int64, bool) {
	value, ok := m[key]
	if !ok {
		return 0, false
	}
	return convertInt64(value)
}

func convertString(obj interface{}) (string, bool) {
	switch val := obj.(type) {
	case string:
		return val, true
	case []byte:
		return string(val), true
	}
	return "", false
}

func convertBool(obj interface{}) (bool, bool) {
	switch val := obj.(type) {
	case bool:
		return val, true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return val != 0, true
	}
	return false, false
}

func convertInt64(obj interface{}) (int64, bool) {
	switch val := obj.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	}
	return 0, false
}

func (*parseSpark) String() string {
	return "parse_spark"
}
