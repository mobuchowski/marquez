package marquez.spark.agent.plan

import marquez.spark.agent.client.DatasetParser.DatasetParseResult
import marquez.spark.agent.client.LineageEvent.{Dataset, DatasetFacet, SchemaDatasetFacet, SchemaField}
import marquez.spark.agent.client.{DatasetParser, LineageEvent}
import marquez.spark.agent.facets.OutputStatisticsFacet
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.marquez.JDBCOperation
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.collection.JavaConversions._

class DatasetCollector {

  /**
   * Collect input datasets found in a given [[QueryExecution]]. This walks the logical plan and extracts any datasets
   * found at the leaf nodes.
   *
   * @param context
   * @param query
   * @return
   */
  def collectInputDatasets(context: SparkContext, queryExec: QueryExecution): Seq[Dataset] = queryExec.logical match {
    // InsertIntoHadoopFsRelationCommand is a special case, as it's matched in extractDatasets explicitly as an output
    // dataset because it contains all of the fields needed to construct the Dataset. To avoid matching it there, extract
    // the input logical plan here.
    case InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, query, _, _, _, _) => extractDatasets(context, query, None)
    case lp@_ => extractDatasets(context, lp, None)
  }


  /**
   * Walk the given logical plan and return a list of [[Dataset]]s found. If a [[OutputStatisticsFacet]] is provided (e.g.,
   * with stats from the executed physical plan) use that. Otherwise, construct one from the estimated statistics
   * provided by the logical plan.
   *
   * @param context
   * @param plan
   * @param statsFacet
   * @return
   */
  def extractDatasets(context: SparkContext, plan: LogicalPlan, statsFacet: Option[OutputStatisticsFacet]): Seq[Dataset] = {
    plan collect {
      // TODO- use partitions in the constructed dataset?
      case c@InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, _, partitionColumns, _, _, _, _, _, catalogTable, _, _) =>
        val dsName = catalogTable.map(c => {
          if (c.qualifiedName.contains(".")) {
            new DatasetParseResult(c.qualifiedName.substring(c.qualifiedName.lastIndexOf(".") + 1),
              c.qualifiedName.substring(0, c.qualifiedName.lastIndexOf(".")))
          } else {
            new DatasetParseResult(c.qualifiedName, "")
          }
        }).getOrElse(DatasetParser.parse(outputPath.toUri))
        val schemaDataset = schemaDatasetFacet(c.schema)
        val stats: OutputStatisticsFacet = getStatsFacet(statsFacet, () => c.stats)
        val datasetFacet = DatasetFacet.builder
          .additional(Map("stats" -> stats))
          .schema(schemaDataset)
          .build()
        buildDataset(dsName, datasetFacet)

      case op@JDBCOperation(schema, _, options) =>
        val schemaDataset = schemaDatasetFacet(schema)
        val stats: OutputStatisticsFacet = getStatsFacet(statsFacet, () => op.stats)
        val datasetFacet = DatasetFacet.builder
          .additional(Map("stats" -> stats, "jdbcOptions" -> options.asProperties))
          .schema(schemaDataset)
          .build()
        buildDataset(DatasetParser.parse(options.url), datasetFacet)

      case r@LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) =>
        val schemaDataset = schemaDatasetFacet(r.schema)
        val stats: OutputStatisticsFacet = getStatsFacet(statsFacet, r.computeStats)
        val datasetFacet = DatasetFacet.builder.additional(Map("stats" -> stats)).schema(schemaDataset).build

        val dsParse = location.rootPaths.map(p => if (p.getFileSystem(context.hadoopConfiguration).getFileStatus(p).isFile) {
          p.getParent
        } else {
          p
        }).distinct
          .map(p => DatasetParser.parse(p.toUri))

        // TODO - should we assume only one base URL for the datasource?
        buildDataset(dsParse.head, datasetFacet)
    }
  }

  private def getStatsFacet(statsFacet: Option[OutputStatisticsFacet], statsProvider: () => Statistics) = {
    statsFacet.getOrElse({
      val statistics = statsProvider()
      new OutputStatisticsFacet(statistics.rowCount.getOrElse(BigInt.apply(0)).longValue(),
        statistics.sizeInBytes.longValue())
    })
  }

  def collectOutputDatasets(context: SparkContext, query: QueryExecution): Seq[Dataset] = {
    val metrics = query.sparkPlan.metrics
    val statsFacet = new OutputStatisticsFacet(
      metrics.getOrElse("numOutputRows", SQLMetrics.createMetric(context, "numOutputRows")).value,
      metrics.getOrElse("numOutputBytes", SQLMetrics.createMetric(context, "numOutputRows")).value)
    val plan = query.logical
    plan match {
      case AppendData(table, _, _) => {
        extractDatasets(context, table, Some(statsFacet))
      }
      case InsertIntoTable(table, partition, _, _, _) =>
        extractDatasets(context, table, Some(statsFacet))

      case i: InsertIntoDir =>
        extractDatasets(context, i, Some(statsFacet))

      case InsertIntoDataSourceCommand(relation, _, _) =>
        extractDatasets(context, relation, Some(statsFacet))

      // TODO- staticPartitions field is present and should be used
      case c: InsertIntoHadoopFsRelationCommand => {
        c.metrics("numOutputRows").value
        extractDatasets(context, c, Some(statsFacet))
      }
      case _ => Seq()
    }
  }

  protected def schemaDatasetFacet(schema: StructType): SchemaDatasetFacet = {
    SchemaDatasetFacet.builder
      ._producer(URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"))
      ._schemaURL(URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/schemaDatasetFacet"))
      .fields(schema.fields.map(f => SchemaField.builder.name(f.name).`type`(f.dataType.typeName).build).toList)
      .build
  }

  protected def buildDataset(result: DatasetParser.DatasetParseResult,
                             datasetFacet: LineageEvent.DatasetFacet): LineageEvent.Dataset =
    Dataset.builder
      .name(result.getName)
      .namespace(result.getNamespace)
      .facets(datasetFacet)
      .build

}
