package scala

import java.util.logging.{Level, Logger}
import scala.flow_controller._

object SparkHWScala {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master\\")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val flowManager = new TripsFlowControllerScalaImp();
    flowManager.handle()
  }

}
