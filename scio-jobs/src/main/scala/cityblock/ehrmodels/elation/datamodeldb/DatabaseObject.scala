package cityblock.ehrmodels.elation.datamodeldb

trait DatabaseObject {
  final protected val project: String = "cbh-db-mirror-prod"
  final protected val dataset: String = "elation_mirror"
  def table: String
  def queryAll: String
}
