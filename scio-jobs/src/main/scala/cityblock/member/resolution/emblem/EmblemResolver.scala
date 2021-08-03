package cityblock.member.resolution.emblem

import java.util.UUID

import cityblock.member.resolution.GenericResolver
import cityblock.member.resolution.models.NodeEntity

import scala.collection.mutable.ListBuffer

/**
 * @author: Haris Nadeem
 */
class EmblemResolver extends GenericResolver[UUID, String](() => UUID.randomUUID()) {

  private val _knownIds = ListBuffer.empty[String]

  def addKnownIds(knownIds: Iterator[(NodeEntity[String], UUID)]): EmblemResolver = {

    knownIds.foreach {
      case (nodeEntity: NodeEntity[String], label: UUID) =>
        this._knownIds.append(nodeEntity.data)
        this.addIdsWithKnownLabel(Seq(nodeEntity), label)
    }

    this
  }

  def addCrosswalk(crosswalk: Iterator[Seq[NodeEntity[String]]]): EmblemResolver = {
    crosswalk.foreach(this.addIds)
    this
  }

  def buildIndex(): List[(String, String, String)] = {

    /*Lookup in a set is O(1), thus more efficient when filtering out fields based on a known set*/
    val k = _knownIds.toSet

    this.getGroups
      .collect {
        case (member, ids) if !(member.pseudo) =>
          for (id <- ids)
            yield (member.identifier.toString, id.groupName, id.data)
      }
      .flatten
      .filter {
        case (_, _, externalId) => !k.contains(externalId)
      }
      .toList
  }

  override def clear(): Unit = {
    _knownIds.clear()
    super.clear()
  }
}
