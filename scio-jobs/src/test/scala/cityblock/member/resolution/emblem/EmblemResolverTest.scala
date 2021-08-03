package cityblock.member.resolution.emblem
import java.util.UUID

import cityblock.member.resolution.models.NodeEntity
import org.scalatest.{FunSpec => AnyFunSpec}

/**
 * @author: Haris Nadeem
 */
class EmblemResolverTest extends AnyFunSpec {

  import EmblemResolverTest._

  describe("the list of all functions") {

    val knownIds: List[(NodeEntity[String], UUID)] = List(
      (NodeEntity("emblem", "M1"), UUID.fromString(MEMBER_ID_1)),
      (NodeEntity("emblem", "M2"), UUID.fromString(MEMBER_ID_1)),
    )

    val knownIds2: List[(NodeEntity[String], UUID)] = List(
      (NodeEntity("emblem", "M1"), UUID.fromString(MEMBER_ID_1)),
      (NodeEntity("emblem", "M4"), UUID.fromString(MEMBER_ID_2)),
      (NodeEntity("medicaidNY", "M6"), UUID.fromString(MEMBER_ID_2)),
    )

    val knownIds3: List[(NodeEntity[String], UUID)] = List(
      (NodeEntity("emblem", "M1"), UUID.fromString(MEMBER_ID_1)),
      (NodeEntity("emblem", "M4"), UUID.fromString(MEMBER_ID_2)),
      (NodeEntity("emblem", "M6"), UUID.fromString(MEMBER_ID_2)),
    )

    val crossWalk: List[Seq[NodeEntity[String]]] = List(
      Seq(
        NodeEntity("emblem", "M1"),
        NodeEntity("emblem", "M3")
      ),
    )

    val crossWalk2: List[Seq[NodeEntity[String]]] = List(
      Seq(
        NodeEntity("emblem", "M6"),
        NodeEntity("emblem", "M7")
      ),
    )

    val crossWalk3: List[Seq[NodeEntity[String]]] = List(
      Seq(
        NodeEntity("emblem", "M2"),
        NodeEntity("medicaidNY", "M6")
      ),
    )

    it("should successfully ignore all known ids and return nothing if no crosswalk Ids are added") {
      val rs: EmblemResolver = new EmblemResolver()

      val results =
        rs.addKnownIds(knownIds.toIterator)
          .buildIndex()

      assert(results.isEmpty)
    }

    it("should successfully ignore non-matching IDs when added a crosswalk") {
      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds.toIterator)
        .addCrosswalk(crossWalk2.toIterator)
        .buildIndex()

      assert(results.isEmpty)
    }

    it("should successfully grab new IDs when added a crosswalk") {
      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds.toIterator)
        .addCrosswalk(crossWalk.toIterator)
        .buildIndex()

      assert(results.size == 1)
      assert(results == List((MEMBER_ID_1, "emblem", "M3")))
    }

    it("should successfully grab new IDs for different carriers") {
      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds.toIterator)
        .addCrosswalk(crossWalk3.toIterator)
        .buildIndex()

      assert(results.size == 1)
      assert(results == List((MEMBER_ID_1, "medicaidNY", "M6")))
    }

    it("should successfully grab new IDs for matching IDs with multiple carriers") {
      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds2.toIterator)
        .addCrosswalk(crossWalk3.toIterator)
        .buildIndex()

      assert(results.size == 1)
      assert(results == List((MEMBER_ID_2, "emblem", "M2")))
    }

    it("should not add members with same ID but different carrier") {
      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds3.toIterator)
        .addCrosswalk(crossWalk3.toIterator)
        .buildIndex()

      assert(results.isEmpty)
    }

    it("should be able to successfully clear out cache when called") {
      val rs: EmblemResolver = new EmblemResolver()

      rs.addKnownIds(knownIds.toIterator)
        .addCrosswalk(crossWalk.toIterator)
        .clear()

      val results = rs.buildIndex()

      assert(results.isEmpty)
    }
  }

  describe("Given a complex situation with multiple cases") {

    val knownIds: Iterator[(NodeEntity[String], UUID)] = List(
      (NodeEntity("emblem", "M1"), UUID.fromString(MEMBER_ID_1)),
      (NodeEntity("emblem", "M2"), UUID.fromString(MEMBER_ID_1)),
      (NodeEntity("emblem", "M4"), UUID.fromString(MEMBER_ID_2)),
      (NodeEntity("medicaidNY", "M6"), UUID.fromString(MEMBER_ID_2)),
    ).toIterator

    val crossWalk: Iterator[Seq[NodeEntity[String]]] = List(
      Seq(
        NodeEntity("emblem", "M1"),
        NodeEntity("emblem", "M3")
      ),
      Seq(
        NodeEntity("emblem", "M4"),
        NodeEntity("emblem", "M5")
      ),
      Seq(
        NodeEntity("emblem", "M6"),
        NodeEntity("emblem", "M7")
      ),
      Seq(
        NodeEntity("medicaidNY", "M6"),
        NodeEntity("medicaidNY", "newMedicaidInsurance")
      ),
    ).toIterator

    it(
      "should successfully grab new IDs, ignore known and non-matching IDs and differentiate similar IDs based on carrier") {

      val rs: EmblemResolver = new EmblemResolver()

      val results = rs
        .addKnownIds(knownIds)
        .addCrosswalk(crossWalk)
        .buildIndex()

      val expected = List(
        (MEMBER_ID_1, "emblem", "M3"),
        (MEMBER_ID_2, "emblem", "M5"),
        (MEMBER_ID_2, "medicaidNY", "newMedicaidInsurance")
      )

      assert(results.size == 3)
      assert(results == expected)
    }
  }

}
object EmblemResolverTest {
  private val MEMBER_ID_1 = "5430a41f-4486-4cd0-a4fe-111020bd21d1"
  private val MEMBER_ID_2 = "080182cc-678b-4625-a066-55800daf5f03"
}
