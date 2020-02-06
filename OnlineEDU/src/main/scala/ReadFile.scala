import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class member(ad_id:String,birthday:String,dn:String,dt:String,email:String,fullname:String,iconurl:String,
                  lastlogin:String,mailaddr:String,memberlevel:String,password:String,paymoney:String,phone:String
                 ,qq:String,register:String,regupdatetime:String,uid:String,unitname:String,userip:String,zipcode:String)
object ReadFile {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReadFile").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        val readFile: RDD[String] = sc.textFile("/user/atguigu/ods/member.log")
        /*readFile.map(data=>{
                    val splits: Array[String] = data.split(",")
                                member(
                                    splits(0),splits(1),splits(2),splits(3),splits(4),splits(5),splits(6).replace(),splits(7),splits(8),
                                  splits(9),splits(10),splits(11),splits(12),splits(13),splits(14),splits(15),splits(16),splits(17),
                                  splits(18),splits(19)

                   )
                })*/

        readFile.foreach(println)
    }

}
