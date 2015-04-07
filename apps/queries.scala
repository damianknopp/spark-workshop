// uses Apache Spark 1.3.0 new DataFrame API
// create a path to the customers.csv dataset, provided by dropbox from the District Data Labs team
// from ./spark-shell, once started run ":load <path to this script>"
// you should see your results count, 28k lines for my file

val split = (line:String) => line.split(",")
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val f = sc.textFile("spark-sample-data/shopping/customers.csv")
case class Customer(id: Long, name: String, email: String, sex: String, date: String, city: String, state: String, zip: String)
val f1 = f.map( _.split(",") )
val f2 = f1.map( x=> Customer( x(0).toLong, x(1), x(2), x(3), x(4), x(5), x(6), x(7) ) )
val schema = sqlContext.createDataFrame(f2)
schema.registerTempTable("customers")
sqlContext.sql("SELECT name FROM customers WHERE state = 'Maryland'").count
