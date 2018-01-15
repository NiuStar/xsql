package xsql


func (s *XSqlOrder) Select2(name string, keys ...string) { //第一个参数为列表名，第二个为参数类型，int，string，float
	reqString := "select "
	s.ClearColType()
	s.SetTableName(name)
	for index, key := range keys {
		reqString += key
		if len(keys) - 1 != index {
			reqString += ","
		}
	}
	reqString += " from " + name
	s.reqString = reqString
	s.tableName = append(s.tableName,name)
}

func (s *XSqlOrder) SelectAll(name string) {
	s.Select2(name,"*")
}