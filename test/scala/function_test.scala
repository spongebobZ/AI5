import com.alibaba.fastjson.JSON

object function_test {
  def main(args: Array[String]): Unit = {
    val s = """{"ageLow":10,"ageHigh":50,"expression":"none"}"""
    println(JSON.parseObject(s).getString("expression"))
  }

}
