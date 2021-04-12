package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
      //(
    //  (-93.63, 33.21)----------------(-93.35, 33.21)
    //                 -              -
    //                 -              -
    //   (-93.63, 33.01)----------------(-93.35, 33.01)
    //input
    // -93.63173,33.0183,-93.359203,33.219456

    val points =  queryRectangle.split(",")
    // // points: Array[String] = Array(-93.63173, 33.0183, -93.359203, 33.219456)

    val float_points = points.map(x => x.toFloat )
    // //float_points: Array[Float] = Array(-93.63173, 33.0183, -93.3592, 33.219456)

    // //bl -> bottom-left, tr -> top-right
    val bl_x = float_points(0)
    val bl_y = float_points(1)
    val tr_x = float_points(2)
    val tr_y = float_points(3)

    val pt = pointString.split(",").map(x => x.toFloat)
    //var pt = pointString.split(",").map(x => x.toFloat)

    // // -88.331492,32.324142
    val x = pt(0)
    val y = pt(1)

    if (x >= bl_x && x <= tr_x && y >= bl_y && y <= tr_y){ true } else{ false } 
    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{

    val points =  queryRectangle.split(",")

    val float_points = points.map(x => x.toFloat )

    // //bl -> bottom-left, tr -> top-right
    val bl_x = float_points(0)
    val bl_y = float_points(1)
    val tr_x = float_points(2)
    val tr_y = float_points(3)

    val pt = pointString.split(",").map(x => x.toFloat)

    val x = pt(0)
    val y = pt(1)

    if (x >= bl_x && x <= tr_x && y >= bl_y && y <= tr_y){ true } else{ false }
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      
      // //-93.63173,33.0183
      val point1 =  pointString1.split(",").map(x => x.toFloat)
      val pt1_x = point1(0)
      val pt1_y = point1(1)

      // //-93.359203,33.219456
      val point2 =  pointString2.split(",").map(x => x.toFloat)
      val pt2_x = point2(0)
      val pt2_y = point2(1)

      // val point_dist = math.sqrt((pt1_x - pt2_x)*(pt1_x - pt2_x)) + ((pt1_y - pt2_y)*(pt1_y - pt2_y))
      // //point_dist: Double = 0.31299359108258296

      val point_dist = math.sqrt( math.pow(pt1_x - pt2_x, 2) + math.pow(pt1_y - pt2_y, 2) )
      // //point_dist: Double = 0.3387275723046348

      if (point_dist > distance) { false } else { true }
      
      })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      
      val point1 =  pointString1.split(",").map(x => x.toFloat)
      val pt1_x = point1(0)
      val pt1_y = point1(1)

      val point2 =  pointString2.split(",").map(x => x.toFloat)
      val pt2_x = point2(0)
      val pt2_y = point2(1)

      val point_dist = math.sqrt((pt1_x - pt2_x)*(pt1_x - pt2_x)) + ((pt1_y - pt2_y)*(pt1_y - pt2_y))

      if (point_dist > distance) { false } else { true }

    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
