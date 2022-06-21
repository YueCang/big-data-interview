import com.interview.UpdateOrderItem.OrderRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import org.scalatest.BeforeAndAfter

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2022/6/21 10:30
 */
class OrderUpdateItemTest {

  var sparkContext:SparkContext = _

  @Before
  def init: Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("test")
    sparkContext = new SparkContext(conf)

  }

  @After
  def close: Unit ={
    sparkContext.stop()
  }

  @Test
  def orderUpdateItemTest: Unit ={
    val orderRDD: RDD[String] = sparkContext.textFile("files")


    val OrderRecordObjects: RDD[OrderRecord] = orderRDD.map(record => {
      val columns: Array[String] = record.split("\\t")
      var dataLong:Long = -1;
      if(!"no value".equals(columns(2))){
        dataLong = columns(2).replace("-","").toLong
      }
      OrderRecord(columns(0), columns(1), columns(2), columns(3),dataLong)
    })

    val orderGroupById: RDD[(String, Iterable[OrderRecord])] = OrderRecordObjects.groupBy(order => order.id)

    val orderSortedAndUpdateItem: RDD[Iterable[OrderRecord]] = orderGroupById.map(f = t => {
      val records: List[OrderRecord] = t._2.toList.sortBy(o => o.date_of_birth_long)

      for (i <- 0 until records.size) {
        var flag = true
        val concurrentRecord: OrderRecord = records(i)

        if (concurrentRecord.date_of_birth_long != -1
          && concurrentRecord.date_of_birth_long < 99999999) {
          for (j <- i+1 until  records.size if flag) {
            if (records(j).date_of_birth_long > 10000000
              && records(j).date_of_birth.contains(concurrentRecord.date_of_birth)) {
              concurrentRecord.date_of_birth_long = 0
              records(j).order_item = records(j).order_item + "," + concurrentRecord.order_item

              flag = false
            }
          }
        }
      }

      records.filter(order => order.date_of_birth_long!=0)
    })

    orderSortedAndUpdateItem.foreach(println)
  }

}
