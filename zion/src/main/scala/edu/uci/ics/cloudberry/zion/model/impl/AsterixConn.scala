package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.util.Logging
import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class AsterixAQLConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn with Logging {

  import AsterixAQLConn._

  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  def postQuery(aql: String): Future[JsValue] = {
    postWithCheckingStatus(aql, (ws: WSResponse) => ws.json, (ws: WSResponse) => defaultQueryResponse)
  }

  def postControl(aql: String): Future[Boolean] = {
    postWithCheckingStatus(aql, (ws: WSResponse) => true, (ws: WSResponse) => false)
  }

  protected def postWithCheckingStatus[T](aql: String, succeedHandler: WSResponse => T, failureHandler: WSResponse => T): Future[T] = {
    post(aql).map { wsResponse =>
      if (wsResponse.status == 200) {
        succeedHandler(wsResponse)
      } else {
        log.error("AQL failed:" + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
  }

  def post(aql: String): Future[WSResponse] = {
    log.debug("AQL:" + aql)
    val f = wSClient.url(url).withRequestTimeout(Duration.Inf).post(aql)
    f.onFailure(wsFailureHandler(aql))
    f
  }

  protected def wsFailureHandler(aql: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => log.error("WS Error:" + aql, e); throw e
  }
}

object AsterixAQLConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}

class AsterixSQLPPConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn with Logging {

  import AsterixSQLPPConn._

  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  def postQuery(query: String, contextID: Option[Int] = None): Future[JsValue] = {
    postWithCheckingStatus(query, (ws: WSResponse) => {
      ws.json.asInstanceOf[JsObject].value("results")
    }, (ws: WSResponse) => defaultQueryResponse, contextID)
  }

  def postControl(query: String): Future[Boolean] = {
    postWithCheckingStatus(query, (ws: WSResponse) => true, (ws: WSResponse) => false)
  }

  protected def postWithCheckingStatus[T](query: String, succeedHandler: WSResponse => T, failureHandler: WSResponse => T, contextID: Option[Int] = None): Future[T] = {
    post(query, contextID).map { wsResponse =>
      if (wsResponse.json.asInstanceOf[JsObject].value.get("status") == Some(JsString("success"))) {
        succeedHandler(wsResponse)
      }
      else {
        log.error("Query failed:" + Json.prettyPrint(wsResponse.json))
        failureHandler(wsResponse)
      }
    }
  }

  def post(query: String, contextID: Option[Int] = None): Future[WSResponse] = {
    log.debug("Query:" + query)
    val f = wSClient.url(url).withRequestTimeout(Duration.Inf).post(params(query, contextID))
    f.onFailure(wsFailureHandler(query))
    f
  }

  def cancel(url: String, contextID: Int): Future[WSResponse] = {
    wSClient.url(url).withQueryString("client_context_id" -> contextID.toString).delete()
  }

  protected def wsFailureHandler(query: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => log.error("WS Error:" + query, e)
      throw e
  }

  protected def params(query: String, contextID: Option[Int]): Map[String, Seq[String]] = {
    Map("client_context_id" -> Seq(contextID.getOrElse(0).toString), "statement" -> Seq(query), "mode" -> Seq("synchronous"), "include-results" -> Seq("true"))
  }

  override def postQuery(statement: String): Future[JsValue] = postQuery(statement, None)

  override def post(statement: String): Future[WSResponse] = post(statement, None)
}

object AsterixSQLPPConn {
  val defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
}
