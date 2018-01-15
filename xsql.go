package xsql
/*

数据库调用方法说明

首先进行InitSql()数据库初始化操作，告诉数据库要去操作哪个数据库

CreateInstance()创建查询对象，每次查询创建一个对象，方便自动化维护查询对象

然后调用增删改查的各种方法，进行语句拼接，也可以直接调用Qurey方法，直接写语句

其中查询方法可以使用三种方式
1、select 把要查询的表名称以及所属字段及字段属性列入其中 如：Select("users","id","int","name","string","phone","string","age","float")
2、先调用SetTableName或SetTableColType或者SetTableColTypeString对要查询的数据库的字段进行初始化设计，方便查询到以后进行序列化的时候自动转换
推荐使用SetTableColType或者SetTableColTypeString，这样减少数据库请求次数
SetTableName会先去调取数据库字段属性，调取成功以后再去调用实际请求的查询语句，这样方便开发，但不利于数据库本身
3、先调用Select2()，不需输入类型，如Select("users","id","name","phone",,"age") 或者 Select("users","*")
然后调用SetTableColType或者SetTableColTypeString对要查询的数据库的字段进行初始化设计，方便查询到以后进行序列化的时候自动转换，如果里面没有采用重命名的字段名称，可以忽略

然后调用Execute方法，进行语句执行，返回结果，也可以直接调用ExecuteForJson，返回json序列化后的字符串
*/
import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"time"
	"nqc.cn/log"
	"sync"
	"strings"
)

const LifeTime int64  =  60 * 60

type XSql struct {
	db         *sql.DB
	name string
	password string
	ip string
	port string
	sqlName string
	mLock *sync.RWMutex
	time_last int64
}

type XSqlOrder struct {
	xs         *XSql
	reqString  string
	//selectKeys map[string]string
	tableName  []string
	colType map[string]string
	ch uint8
}

func CreateInstance(xs *XSql) *XSqlOrder {
	o := new (XSqlOrder)
	o.xs = xs
	//o.selectKeys = make(map[string]string)
	o.colType = make(map[string]string)
	return o
}

func Substr(str string, start, length int) string {
	rs := []rune(str)
	rl := len(rs)
	end := 0

	if start < 0 {
		start = rl - 1 + start
	}
	end = start + length

	if start > end {
		start, end = end, start
	}

	if start < 0 {
		start = 0
	}
	if start > rl {
		start = rl
	}
	if end < 0 {
		end = 0
	}
	if end > rl {
		end = rl
	}
	return string(rs[start:end])
}

func InitSql(name string, password string, ip string, port string, sqlName string) *XSql {
	db := createDB(name,password,ip,port,sqlName)
	fmt.Println("初始化数据库成功")
	s := new(XSql)
	s.mLock = new(sync.RWMutex)
	s.db = db
	s.name = name
	s.password = password
	s.ip = ip
	s.port = port
	s.sqlName = sqlName
	s.time_last = time.Now().Unix()
	//s.ch = make(chan uint8,100)

	//go timer(s)

	return s
}

func createDB(name string, password string, ip string, port string, sqlName string) *sql.DB {
	db, err := sql.Open("mysql", name+":"+password+"@tcp("+ip+":"+port+")/"+sqlName+"?charset=utf8")
	//fmt.Println(name+":"+password+"@tcp("+ip+":"+port+")/"+sqlName+"?charset=utf8")
	checkErr(err)
	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	db.SetConnMaxLifetime(10 * time.Minute)
	err = db.Ping()

	checkErr(err)


	return db
}

func timer(s *XSqlOrder) {
	//nt := int64(time.Now().Unix())
	/*timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			{
				v := <- s.ch
				fmt.Println(v)
				//s.createNewDB()
				if v == 1 {
					return
				}
			}
		}
	}*/
	//fmt.Println("timer")
	for i := 0 ; i < 1000 ; i ++ {
		//fmt.Println("timer:" , i)
		//s.createNewDB()
		if s.ch == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("重新生成:")
	s.xs.db.Close()
	db := createDB(s.xs.name,s.xs.password,s.xs.ip,s.xs.port,s.xs.sqlName)
	s.xs.db = db
	s.xs.time_last = time.Now().Unix()
	fmt.Println("重新生成:OK")
}

func (s *XSqlOrder) ClearColType() {
	s.colType = make(map[string]string)
}

func (s *XSqlOrder) Select(name string, keys ...string) { //第一个参数为列表名，第二个为参数类型，int，string，float
	//parses(keys)
	fmt.Println("SELECT..........")
	reqString := "select "
	s.ClearColType()
	//selectKeys = keys
	var nextKey string
	for index, key := range keys {
		if index%2 == 0 {
			reqString += key + ","
			nextKey = strings.ToLower(key)
		} else {
			s.colType[nextKey] = strings.ToLower(key)
		}
	}

	reqString = Substr(reqString, 0, len(reqString)-1)
	reqString += " from " + name

	s.reqString = reqString
	//s.selectKeys = selectKeys
	s.tableName = append(s.tableName,name)
	fmt.Println(reqString)
}
func (s *XSqlOrder) Insert(values map[string]interface{}, name string) {

	reqString := "INSERT INTO " + name + " ("
	//s.selectKeys = make(map[string]string)

	var valueString string = ""
	//sorted_keys := make([]string, 0)
	var index = 0
	for key, value := range values {
		index ++
		reqString += key
		if index != len(values) {
			reqString += ","
		}
		//valueString += value + ","
		//value := values[k]
		switch value.(type) {
		case int:
			{

				valueString += strconv.FormatInt(int64(value.(int)), 10)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case int64:
			{

				valueString += strconv.FormatInt(value.(int64), 10)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case float32:
			{

				valueString += strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case float64:
			{

				valueString += strconv.FormatFloat(value.(float64), 'f', 6, 64)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case string:
			{

				valueString += "\"" + value.(string) + "\""
				if index != len(values) {
					valueString += ","
				}

			}
			break
		case []byte:
			{

				valueString += "\"" + string(value.([]byte)) + "\""
				if index != len(values) {
					valueString += ","
				}
			}
			break
		}
		//sorted_keys = append(sorted_keys, key)
	}
	reqString += ") VALUES ( " + valueString
	//s.reqString = Substr(reqString, 0, len(reqString)-1)
	s.reqString = reqString + ")"

}

func (s *XSqlOrder) Insert_(values map[string]interface{}, name string) {

	reqString := "INSERT INTO " + name + " ("
	//s.selectKeys = make(map[string]string)

	var valueString string = ""
	//sorted_keys := make([]string, 0)
	var index = 0
	for key, value := range values {
		index ++
		reqString += key
		if index != len(values) {
			reqString += ","
		}
		//valueString += value + ","
		//value := values[k]
		switch value.(type) {
		case int:
			{

				valueString += strconv.FormatInt(int64(value.(int)), 10)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case int64:
			{

				valueString += strconv.FormatInt(value.(int64), 10)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case float32:
			{

				valueString += strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case float64:
			{

				valueString += strconv.FormatFloat(value.(float64), 'f', 6, 64)
				if index != len(values) {
					valueString += ","
				}
			}
			break
		case string:
			{

				valueString += "'" + value.(string) + "'"
				if index != len(values) {
					valueString += ","
				}

			}
			break
		case []byte:
			{

				valueString += "\"" + string(value.([]byte)) + "\""
				if index != len(values) {
					valueString += ","
				}
			}
			break
		}
		//sorted_keys = append(sorted_keys, key)
	}
	reqString += ") VALUES ( " + valueString
	//s.reqString = Substr(reqString, 0, len(reqString)-1)
	s.reqString = reqString + ")"

}


func (s *XSqlOrder) MulitInsert(list []map[string]interface{}, name string) {

	reqString := "INSERT INTO " + name + " ("
	//s.selectKeys = make(map[string]string)

	var valueString string = ""
	//sorted_keys := make([]string, 0)
	var key_list []string
	if len(list) > 0 {
		var index = 0;
		for key, _ := range list[0] {
			key_list = append(key_list,key)
			index++
			reqString += key
			if index != len(list[0]) {
				reqString += ","
			}
		}
	}

	for index_k , values := range list {
		index_k++
		//var index = 0
		for index,key_value := range key_list {
			value := values[key_value]
			index ++
			switch value.(type) {
			case int:
				{

					valueString += strconv.FormatInt(int64(value.(int)), 10)
					if index != len(values) {
						valueString += ","
					}
				}
				break
			case int64:
				{

					valueString += strconv.FormatInt(value.(int64), 10)
					if index != len(values) {
						valueString += ","
					}
				}
				break
			case float32:
				{

					valueString += strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
					if index != len(values) {
						valueString += ","
					}
				}
				break
			case float64:
				{

					valueString += strconv.FormatFloat(value.(float64), 'f', 6, 64)
					if index != len(values) {
						valueString += ","
					}
				}
				break
			case string:
				{

					valueString += "\"" + value.(string) + "\""
					if index != len(values) {
						valueString += ","
					}
				}
				break
			case []byte:
				{

					valueString += "\"" + string(value.([]byte)) + "\""
					if index != len(values) {
						valueString += ","
					}
				}
				break
			}
		}

		if index_k != len(list)  {
			valueString += "),( "
		}
	}
	fmt.Println("reqString: ",reqString)
	fmt.Println("valueString: ",valueString)

	reqString += ") VALUES ( " + valueString
	fmt.Println("reqString: ",reqString)
	//s.reqString = Substr(reqString, 0, len(reqString)-1)
	s.reqString = reqString + ")"

}

func (s *XSqlOrder) Update(values map[string]interface{}, name string) {

	reqString := "UPDATE " + name + " SET "
	//s.selectKeys = make(map[string]string)
	var index = 0
	for key, value := range values {
		index++
		switch value.(type) {
		case int:
			{
				reqString += key + "=" + strconv.FormatInt(int64(value.(int)), 10)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case int64:
			{
				reqString += key + "=" + strconv.FormatInt(value.(int64), 10)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case float32:
			{
				reqString += key + "=" + strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case float64:
			{
				reqString += key + "=" + strconv.FormatFloat(value.(float64), 'f', 6, 64)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case string:
			{
				reqString += key + "=" + "\"" + value.(string) + "\""
				if index != len(values) {
					reqString += ","
				}
			}
			break
		case []byte:
			{
				reqString += key + "=" + "\"" + string(value.([]byte)) + "\""
				if index != len(values) {
					reqString += ","
				}
			}
			break
		}
	}
	s.reqString = reqString
	//s.reqString = Substr(reqString, 0, len(reqString)-2)

}

//与上一个方法差距在case string ，将转义符双引号改为了单引号
func (s *XSqlOrder) Update_(values map[string]interface{}, name string) {

	reqString := "UPDATE " + name + " SET "
	//s.selectKeys = make(map[string]string)
	var index = 0
	for key, value := range values {
		index++
		switch value.(type) {
		case int:
			{
				reqString += key + "=" + strconv.FormatInt(int64(value.(int)), 10)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case int64:
			{
				reqString += key + "=" + strconv.FormatInt(value.(int64), 10)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case float32:
			{
				reqString += key + "=" + strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case float64:
			{
				reqString += key + "=" + strconv.FormatFloat(value.(float64), 'f', 6, 64)
				if index != len(values) {
					reqString += ", "
				}
			}
			break
		case string:
			{
				reqString += key + "=" + "'" + value.(string) + "'"
				if index != len(values) {
					reqString += ","
				}
			}
			break
		case []byte:
			{
				reqString += key + "=" + "\"" + string(value.([]byte)) + "\""
				if index != len(values) {
					reqString += ","
				}
			}
			break
		}
	}
	s.reqString = reqString
	//s.reqString = Substr(reqString, 0, len(reqString)-2)

}



func (s *XSqlOrder) Delete(name string) {
	s.reqString = "DELETE FROM " + name
	//s.selectKeys = make(map[string]string)
}
func (s *XSqlOrder) Where(values map[string]interface{}) {
	reqString := " where "
	var index = 0
	for key, value := range values {
		index ++
		switch value.(type) {
		case int:
			{
				reqString += key + "=" + strconv.FormatInt(int64(value.(int)), 10)
			}
			break
		case int64:
			{
				reqString += key + "=" + strconv.FormatInt(value.(int64), 10)
			}
			break
		case float32:
			{
				reqString += key + "=" + strconv.FormatFloat(float64(value.(float32)), 'f', 0, 32)
			}
			break
		case float64:
			{
				reqString += key + "=" + strconv.FormatFloat(value.(float64), 'f', 0, 64)
			}
			break
		case string:
			{
				reqString += key + "=" + "\"" + value.(string)
			}
			break
		case []byte:
			{
				reqString += key + "=" + "\"" + string(value.([]byte))
			}
			break
		}
		if index != len(values) {
			reqString += " AND "
		}
	}
	//s.reqString = Substr(reqString, 0, len(reqString)-4)
}
func (s *XSqlOrder) AddSuf(suffixes string) { //添加sql尾部参数
	s.reqString += " " + suffixes

}
func (s *XSqlOrder) Count(name string) {
	s.colType = make(map[string]string)
	s.reqString = "select count(*) as count from " + name
	s.colType["count"] = "int"
}

func (s *XSqlOrder) CountMore(name string,tag string) {
	s.colType = make(map[string]string)
	s.reqString = "select count(" + tag + ") as count from " + name
	s.colType["count"] = "int"
}

func (s *XSqlOrder) Value() int64 {
	list := s.Execute()
	for _, value := range list {
		for _, value_c := range value {
			return value_c.(int64)
		}
	}
	return 0
}
func (s *XSqlOrder) Qurey(suffixes string) { //执行sql语句
	//s.colType = make(map[string]string)
	s.reqString = suffixes
}

func checkErr(err error) {
	if err != nil {
		log.Write(err)
	}
}
func (s *XSqlOrder) ExecuteForJson() string { //执行sql语句得到json

	body, err := json.Marshal(s.Execute())
	if err != nil {
		fmt.Println(err)
	}
	return string(body)
}

/*
func ExecuteForRows() *sql.Rows { //执行sql语句
	fmt.Println("执行sql语句: " + reqString)
	rows, err := db.Query(reqString)
	defer rows.Close()
	checkErr(err)
	return rows
}*/
func (s *XSqlOrder) GetSQLString() string {
	return s.reqString
}

func (s *XSqlOrder)createNewDB(){
	s.xs.mLock.RLock()
	tm := time.Now().Unix()
	if tm - s.xs.time_last > LifeTime {
		s.xs.db.Close()
		db := createDB(s.xs.name,s.xs.password,s.xs.ip,s.xs.port,s.xs.sqlName)
		s.xs.db = db
	}
	s.xs.time_last = time.Now().Unix()
	s.xs.mLock.RUnlock()
}


func (s *XSqlOrder) ExecuteNoResult() {
	//SQL
	fmt.Println("ExecuteNoResult执行sql语句: " + s.reqString)
	s.ch = 0
	s.xs.mLock.RLock()
	go timer(s)
	rows, _ := s.xs.db.Query(s.reqString)
	s.xs.mLock.RUnlock()
	s.ch = 1
	rows.Close()
}


func (s *XSqlOrder) Execute2() (results []map[string]interface{}) { //SQL

	defer func () {
		if err := recover(); err != nil {
			fmt.Println("数据库执行错误：",err)
		}

	}()

	fmt.Println("Execute执行sql语句: " + s.reqString)

	s.ch = 0

	s.xs.mLock.RLock()
	go timer(s)

	rows, err := s.xs.db.Query(s.reqString)

	s.xs.mLock.RUnlock()
	s.ch = 1
	if err != nil {
		fmt.Println("error: ",err)
		s.xs.mLock.RLock()
		s.xs.db.Close()
		db := createDB(s.xs.name,s.xs.password,s.xs.ip,s.xs.port,s.xs.sqlName)
		s.xs.db = db
		s.xs.time_last = time.Now().Unix()

		rows, err = s.xs.db.Query(s.reqString)
		checkErr(err)
		return nil
	}

	defer rows.Close()

	columns, err2 := rows.Columns()
	if err2 != nil {
		log.Write(err2) // proper error handling instead of panic in your app
		return nil
	}

	if len(columns) <= 0 {
		return nil
	}

	// Make a slice for the values
	values := make([]interface{}, len(columns))
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]

	}
	//var results []map[string]interface{}

	for rows.Next() {

		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		t := make(map[string]interface{})
		//fmt.Println(len(selectKeys))
		for i, col := range values {
			//	fmt.Println(i)
			//fmt.Println(col)
			//fmt.Println(selectKeys[columns[i]])
			if col == nil {
				t[columns[i]] = nil
			} else {
				switch s.colType[strings.ToLower(columns[i])] {
				case "int":
					{

						t[columns[i]] = byte2Int(col.([]byte))
					}
					break
				case "float":
					{
						fmt.Println(columns[i])
						t[columns[i]] = byte2Float(col.([]byte))
					}
					break
				case "string":
					{

						t[columns[i]] = byte2String(col.([]byte))
					}
					break
				default:
					{

					}
					break
				}
			}

		}
		results = append(results, t)

	}
	//rows.Close()

	return results

}
func byte2Int(value []byte) int64 {

	result, err := strconv.ParseInt(string(value), 10, 64)
	checkErr(err)
	return result
}
func byte2Float(value []byte) float64 {

	result, err := strconv.ParseFloat(string(value), 64)
	checkErr(err)
	return result
}

func byte2String(value []byte) string {
	return string(value)
}

func (s *XSqlOrder)SetTableName(name ...string) map[string]string {
	s.tableName = name
	var sqlString string = "SELECT column_name,data_type FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME='"
	for i, value := range name {
		sqlString += value
		if i != len(name) - 1 {
			sqlString +=  "' OR TABLE_NAME='"
		}
	}
	sqlString +=  "' "
	s.ch = 0
	s.xs.mLock.RLock()
	rows,err := s.xs.db.Query(sqlString)

	s.xs.mLock.RUnlock()
	s.ch = 1
	if err != nil {
		fmt.Println("error: ",err)
		s.xs.mLock.RLock()
		s.xs.db.Close()
		db := createDB(s.xs.name,s.xs.password,s.xs.ip,s.xs.port,s.xs.sqlName)
		s.xs.db = db
		s.xs.time_last = time.Now().Unix()

		rows, err = s.xs.db.Query(s.reqString)
		defer rows.Close()
		checkErr(err)
		return nil
	}
	defer rows.Close()
	//t := make(map[string]string)
	for rows.Next() {
		var column_name string
		var data_type string
		err = rows.Scan(&column_name,&data_type)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		if strings.Contains(data_type,"int")  {
			s.colType[column_name] = "int"
		} else if strings.Contains(data_type,"float")  {
			s.colType[column_name] = "float"
		} else if strings.Contains(data_type,"bool")  {
			s.colType[column_name] = "bool"
		} else {
			s.colType[column_name] = "string"
		}
	}
	//rows.Close()
	//s.colType = t
	return s.colType
}
//int  float string
func (s *XSqlOrder) SetTableColType(data_type map[string]string) {
	//SQL
	for key,value := range data_type {
		s.colType[strings.ToLower(key)] = strings.ToLower(value)
	}
}

func (s *XSqlOrder) SetTableColTypeString(data_types ...string) {
	var nextKey string
	for index, key := range data_types {
		if index%2 == 0 {
			nextKey = strings.ToLower(key)
		} else {
			s.colType[nextKey] = strings.ToLower(key)
		}
	}
}

func (s *XSqlOrder) Execute() (results []map[string]interface{}) { //SQL

	defer func () {
		if err := recover(); err != nil {
			fmt.Println("数据库执行错误：",err)
		}
	}()

	fmt.Println("Execute执行sql语句: " + s.reqString)

	s.ch = 0
	s.xs.mLock.RLock()

	go timer(s)

	rows, err := s.xs.db.Query(s.reqString)


	s.xs.mLock.RUnlock()
	s.ch = 1
	if err != nil {
		fmt.Println("error: ",err)
		s.xs.mLock.RLock()
		s.xs.db.Close()
		db := createDB(s.xs.name,s.xs.password,s.xs.ip,s.xs.port,s.xs.sqlName)
		s.xs.db = db
		s.xs.time_last = time.Now().Unix()

		rows, err = s.xs.db.Query(s.reqString)
		defer rows.Close()
		checkErr(err)
		return nil
	}

	defer rows.Close()
	columns, err2 := rows.Columns()
	if err2 != nil {
		log.Write(err2) // proper error handling instead of panic in your app
		return nil
	}

	if len(columns) <= 0 {
		return nil
	}

	// Make a slice for the values
	values := make([]interface{}, len(columns))
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]

		//fmt.Println(columns[i])
	}
	//return nil
	//var results []map[string]interface{}

	for rows.Next() {

		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		t := make(map[string]interface{})
		for i, col := range values {

			if col == nil {
				t[columns[i]] = nil
			} else {
				switch s.colType[strings.ToLower(columns[i])] {
				case "int":
					{

						t[columns[i]] = byte2Int(col.([]byte))
					}
					break
				case "float":
					{
						fmt.Println(columns[i])
						t[columns[i]] = byte2Float(col.([]byte))
					}
					break
				case "string":
					{

						t[columns[i]] = byte2String(col.([]byte))
					}
					break
				default:
					{
						t[columns[i]] = getInitValue(col.([]byte))
					}
					break
				}
			}

		}
		results = append(results, t)

	}

	fmt.Println("results:",results)
	return results
}

func getInitValue(pval []byte) interface{} {
	result_int,ok := ParseInt(pval)
	if !ok {
		result_float, ok := ParseFloat(pval)
		if !ok {
			fmt.Println("string")
			return string(pval)
		}
		fmt.Println("float")
		return result_float
	}else{
		s := string(pval)
		a := strings.Split(s,"0")
		if strings.EqualFold(a[0],""){
			return string(pval)
		}
		fmt.Println("int")
		return result_int
	}
}

func ParseInt(value []byte) (int64,bool) {
	result, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0,false
	}
	return result,true
}

func ParseFloat(value []byte) (float64,bool) {
	result, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return 0,false
	}
	return result,true
}