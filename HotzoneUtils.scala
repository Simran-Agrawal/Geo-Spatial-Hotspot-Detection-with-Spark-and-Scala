package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    // YOU NEED TO CHANGE THIS PART        
    val rect = queryRectangle.split(",").map(_.toDouble)
    val point = pointString.split(",").map(_.toDouble)

    val xMin = Math.min(rect(0), rect(2))
    val xMax = Math.max(rect(0), rect(2))
    val yMin = Math.min(rect(1), rect(3))
    val yMax = Math.max(rect(1), rect(3))

    val px = point(0)
    val py = point(1)

    px >= xMin && px <= xMax && py >= yMin && py <= yMax
    }
}
