package edu.uci.ics.cloudberry.zion.experiment

import edu.uci.ics.cloudberry.zion.model.impl.SQLPPGenerator
import edu.uci.ics.cloudberry.zion.model.schema.FilterStatement

/**
  * Created by jianfeng on 4/17/17.
  */
class NgramSQLPPGenerator extends SQLPPGenerator{
  override protected def parseTextRelation(filter: FilterStatement, fieldExpr: String): String = {
    val first = s"${typeImpl.similarityJaccard}(${typeImpl.wordTokens}($fieldExpr), ${typeImpl.wordTokens}('${filter.values.head}')) > 0.0"
    val rest = filter.values.tail.map(keyword => s"""and ${typeImpl.contains}($fieldExpr, "$keyword")""")
    (first +: rest).mkString("\n")
  }
}
