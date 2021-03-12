package org.apache.spark.sql.marquez

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.types.StructType

/**
 * Pattern matching a [[LogicalRelation]] over a [[JDBCRelation]]. Since [[JDBCRelation]] is package private, we
 * need this extractor to be in a subpackage of [[org.apache.spark.sql]].
 */
object JDBCOperation {

  /**
   * Return a schema, partition columns, and [[JDBCOptions]] tuple, if the pattern matches.
   * @param plan
   * @return
   */
  def unapply(plan: LogicalRelation): Option[(StructType, Array[Partition], JDBCOptions)] = {
    plan match {
      case LogicalRelation(JDBCRelation(schema, partitions, options), _, _, _) => Some((schema, partitions, options))
      case _ => None
    }
  }

}
