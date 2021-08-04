package cityblock.member.resolution

import scala.annotation.tailrec
import scala.collection.mutable
import cityblock.member.resolution.models.{Label, NodeEntity}

/**
 * @author: Haris Nadeem
 */
class GenericResolver[S, T](private val labelFn: () => S) {

  type L = Label[S]
  type I = NodeEntity[T]

  private val labelIndex = mutable.Map[L, mutable.Set[I]]()
  private val idIndex = mutable.Map[I, L]()

  /* WARNING: if the number of labels stored exceed the Space of Label Generation, this method will hit an infinite loop */
  @tailrec
  private def mkLabel(): L = {
    val label = Label(identifier = labelFn(), pseudo = true)
    if (labelIndex.contains(label)) {
      mkLabel()
    } else {
      label
    }
  }

  private def updateLabel(label: L, newIds: Iterable[I]): Unit =
    labelIndex.get(label) match {
      case Some(existingIds) => newIds.foreach(existingIds.add)
      case _ =>
        val existingIds = labelIndex.put(label, GenericResolver.toMutableSet(newIds))
        require(existingIds.isEmpty)
    }

  private def updateReverseLookup(label: L, newKeys: Iterable[I]): Unit =
    newKeys.foreach(idIndex.put(_, label))

  private def update(label: L, newIds: Iterable[I]): L = {
    updateLabel(label, newIds)
    updateReverseLookup(label, newIds)
    label
  }

  /* Check if there exists a Label that is not pseudo generated */
  @throws[java.lang.IllegalStateException]
  private def advancedMergeLabels(sources: Seq[L] = Seq()): L = {

    require(sources.flatMap(labelIndex.get).nonEmpty,
            "Attempted to merge ids that aren't in the labelIndex")

    val (generatedLabel, predefinedLabel) = sources.partition(_.pseudo)

    /* There should only ever be one predefined label. A predefined label is one that is known to be representative and distinct for a set of IDs. */
    predefinedLabel.length match {
      case 0 => mergeLabels(sources.head, sources.tail)
      case 1 => mergeLabels(predefinedLabel.head, generatedLabel)
      case _ =>
        throw new IllegalStateException(
          s"Conflict of Labels. Two labels, where pseudo = false, cannot map to the same entity. Labels: $sources. Label Issue: $predefinedLabel")
    }
  }

  private def mergeLabels(destination: L, sources: Seq[L] = Seq()): L =
    sources.length match {
      case 0 => destination
      case _ =>
        val idsToMigrate = sources.flatMap(labelIndex.apply)
        sources.foreach(labelIndex.remove)
        update(destination, idsToMigrate)
    }

  /**
   * Assigns a label to one or more ids that represent the same entity.
   *
   * If no label exists for any id in `ids`, a new label is created.
   * If only one id is labeled, the others ids are given that same label.
   * If the ids map to two or more previously distinct labels, all ids are labeled with the first label found (when
   * iterating through the ids). The other labels are then deleted.
   *
   * @param ids list of ids that represent the same entity
   * @return the label that these ids now fall under, or None if no ids are supplied
   */
  @throws[java.lang.Exception]
  def addIds(ids: Seq[I]): Option[L] =
    if (ids.nonEmpty) {
      val lookups = ids.map(key => (key, idIndex.get(key)))
      val labels = lookups.flatMap {
        case (_, label) => label
      }.distinct

      val l = labels.length match {
        case 0 => update(mkLabel(), ids)
        case 1 => update(labels.head, ids)
        case _ =>
          val label = advancedMergeLabels(labels)
          val unlabeledKeys = lookups.collect {
            case (key, None) => key
          }
          update(label, unlabeledKeys)
      }
      Option(l)
    } else {
      None
    }

  /**
   * @param ids a set of Ids that belong to the same entity
   * @param knownLabel the corresponding label already known for a set of Ids
   * @return the label that these ids fall under, or None if no ids are supplied
   */
  @throws[java.lang.Exception]
  def addIdsWithKnownLabel(ids: Seq[I], knownLabel: S): Option[L] = {
    val label = Label(knownLabel, pseudo = false)
    if (ids.nonEmpty) {
      val idLookup = ids.map(key => (key, idIndex.get(key)))
      val allAssociatedLabels = idLookup.flatMap {
        case (_, label) => label
      }.distinct

      val l = allAssociatedLabels.length match {
        case 0 => update(label, ids)
        case _ =>
          val allLabels = allAssociatedLabels ++ Seq(label)
          val correctLabel = advancedMergeLabels(allLabels)
          val unlabeledKeys = idLookup.collect {
            case (key, None) => key
          }
          update(correctLabel, unlabeledKeys)
      }
      Option(l)
    } else {
      None
    }
  }

  /**
   * Retrieve the groups of ids along with their labels.
   * @return [[Map]] of labels to id groups
   */
  def getGroups: Map[L, Set[I]] =
    labelIndex.map {
      case (label, ids) => (label, ids.toSet)
    }.toMap

  def clear(): Unit = {
    idIndex.clear()
    labelIndex.clear()
  }
}

object GenericResolver {

  private def toMutableSet[A](iter: Iterable[A]): mutable.Set[A] = {
    val mSet = mutable.Set[A]()
    iter.foreach(mSet.add)
    mSet
  }

}
