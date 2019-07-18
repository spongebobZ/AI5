import com.alibaba.fastjson.JSON

object function_test {
  def main(args: Array[String]): Unit = {
    val jsonstr = """{"name":"hrr","age":18,"tall":1.67}"""
    val j = JSON.parseObject(jsonstr)
    println(j.getString("name"))
    println(j.getString("age"))
    println(j.getString("tall"))
  }

}
