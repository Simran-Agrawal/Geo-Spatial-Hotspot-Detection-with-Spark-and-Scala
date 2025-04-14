package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX: Int = (-74.50 / HotcellUtils.coordinateStep).toInt
    val maxX: Int = (-73.70 / HotcellUtils.coordinateStep).toInt
    val minY: Int = (40.50 / HotcellUtils.coordinateStep).toInt
    val maxY: Int = (40.90 / HotcellUtils.coordinateStep).toInt
    val minZ: Int = 1
    val maxZ: Int = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)


    // YOU NEED TO CHANGE THIS PART
    import spark.implicits._
    // 1. Compute the number of points in each existing cell.
    val cellPoints = pickupInfo.groupBy("x", "y", "z")
      .agg(count("*").alias("point"))
    
    // 2. Generate a DataFrame for all possible cell combinations based on the grid limits.
    val xRange = (minX.toInt to maxX.toInt).toDF("x")
    val yRange = (minY.toInt to maxY.toInt).toDF("y")
    val zRange = (minZ to maxZ).toDF("z")
    val allCells = xRange.crossJoin(yRange).crossJoin(zRange)
    
    // 3. Left join the grid with the actual cell counts – assign zero for cells with no points.
    val allCellsWithPoints = allCells.join(cellPoints, Seq("x", "y", "z"), "left_outer")
      .na.fill(0, Seq("point"))
    
    // 4. Compute global statistics: total points, the average (X̄), and standard deviation S.
    val totalPoints = allCellsWithPoints.agg(sum("point")).first().getLong(0)
    val Xbar = totalPoints.toDouble / numCells
    
    val sumX2 = allCellsWithPoints.agg(sum(pow(col("point"), 2))).first().getDouble(0)
    val S = math.sqrt(sumX2 / numCells - Xbar * Xbar)
    
    // 5. For each cell, compute the sum of counts in its neighboring cells (neighbors are those cells 
    //    that are within a difference of 1 in all dimensions) and count the neighbors.
    val neighborJoined = allCellsWithPoints.as("a")
      .join(allCellsWithPoints.as("b"),
        (abs(col("a.x") - col("b.x")) <= 1) &&
        (abs(col("a.y") - col("b.y")) <= 1) &&
        (abs(col("a.z") - col("b.z")) <= 1)
      )
      .groupBy(col("a.x").alias("x"), col("a.y").alias("y"), col("a.z").alias("z"))
      .agg(
         sum(col("b.point")).alias("sum_points"),
         count(col("b.point")).alias("neighbor_count")
      )
    
    // 6. Compute the Gi* score for each cell.
    val giDf = neighborJoined.withColumn("gi_score",
      (col("sum_points") - lit(Xbar) * col("neighbor_count")) /
      (lit(S) * sqrt((lit(numCells) * col("neighbor_count") - pow(col("neighbor_count"), 2)) / lit(numCells - 1)))
    )
    
    // 7. Select the top 50 cells (sorted by Gi* descending). Do NOT output the Gi* score.
    val result = giDf.orderBy(col("gi_score").desc).limit(50)
      .select("x", "y", "z")
    
    return result
  }
}