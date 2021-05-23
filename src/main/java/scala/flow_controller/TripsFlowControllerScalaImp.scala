package scala.flow_controller

import scala.handler.TripsHandlerScala

class TripsFlowControllerScalaImp extends TripsFlowControllerScala
{
  override def handle(): Unit = {
    val tripsHandler = new TripsHandlerScala
    tripsHandler.countNumberOfLines()
    val persist = tripsHandler.filterOutCityToCheck()
    tripsHandler.calculateNumberOfTripsLongerThanXKm(persist)
    tripsHandler.calculateTotalTripsDistanceToSpecificCity(persist)
    tripsHandler.findBestDrivers()
  }


}
