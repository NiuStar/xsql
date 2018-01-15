# xsql

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
    
    
#示例代码

	s := xsql.CreateInstance(xs)
	s.Select("stang_wx_game","score","int")
	s.AddSuf(" where score <= " + score)
	list := s.Execute()


