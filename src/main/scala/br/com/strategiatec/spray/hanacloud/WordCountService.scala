package br.com.strategiatec.spray.hanacloud

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.CountDownLatch
import akka.actor.Actor
import scala.collection.mutable.HashMap
import akka.actor.Props
import akka.routing.RoundRobinRouter

class MasterActor(latch: CountDownLatch) extends Actor {
  val mapActor = context.actorOf(Props[MapActor].withRouter(RoundRobinRouter(nrOfInstances =5)), name = "map")
  val reduceActor = context.actorOf(Props[ReduceActor].withRouter(RoundRobinRouter(nrOfInstances = 5)), name = "reduce")
  val aggregateActor = context.actorOf(Props(new AggregateActor(latch)), name = "aggregate")
  
  def receive: Receive = {
    case line: String => mapActor ! line
    case mapData: MapData => reduceActor ! mapData
    case reduceData: ReduceData => aggregateActor ! reduceData
    case Result => aggregateActor forward Result
  }
}

class MapActor extends Actor {
  val STOP_WORDS = List("a", "am", "an", "and", "are", "as", "at",
    "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  
  def receive: Receive = {
    case message: String =>
    sender ! countWords(message)
  }

  def countWords(line: String): MapData = MapData {
    line.split("""\s+""").foldLeft(ArrayBuffer.empty[WordCount]) {
      (index, word) =>
        if (!STOP_WORDS.contains(word.toLowerCase()))
          index += WordCount(word.toLowerCase, 1)
        else
          index
    }
  }
}

class AggregateActor(latch: CountDownLatch) extends Actor {
  val finalReducedMap = new HashMap[String, Int]

  def receive: Receive = {
    case ReduceData(reduceDataMap) =>
      aggregateInMemoryReduce(reduceDataMap)
      latch.countDown()
    case Result => 
      sender ! finalReducedMap
  }

  def aggregateInMemoryReduce(reducedList: Map[String, Int]): Unit = {
    for ((key, value) <- reducedList) {
      if (finalReducedMap contains key)
        finalReducedMap(key) = (value + finalReducedMap.get(key).get)
      else
        finalReducedMap += (key -> value)
    }
  }
}

class ReduceActor extends Actor {
	def receive: Receive = {
	  case MapData(dataList) =>
	    sender ! reduce(dataList)
	}
	
	def reduce(words: IndexedSeq[WordCount]): ReduceData = ReduceData {
	  words.foldLeft(Map.empty[String, Int]){
	    (index, words) => 
	      if (index contains words.word)
	        index + (words.word -> (index.get(words.word).get + 1))
	      else 
	        index + (words.word -> 1)
	  }
	}
}

sealed trait MapReduceMessage
case class WordCount(word: String, count: Int) extends MapReduceMessage
case class MapData(dataList: ArrayBuffer[WordCount]) extends MapReduceMessage
case class ReduceData(reduceDataMap: Map[String, Int]) extends MapReduceMessage
case class Result() extends MapReduceMessage
