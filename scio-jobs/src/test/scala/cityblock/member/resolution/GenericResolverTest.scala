package cityblock.member.resolution
import cityblock.member.resolution.SimpleGenerator._
import cityblock.member.resolution.models.NodeEntity
import cityblock.utilities.Loggable
import org.scalatest.{FlatSpec => AnyFlatSpec}

/**
 * @author: Haris Nadeem
 */
class GenericResolverTest extends AnyFlatSpec with Loggable {

  val sng = new SimpleNumberGenerator()

  val label1 = 1000
  val label2 = -1000

  it should "Generate a unique Label for each GroupName" in {

    val srg = new SimpleRandomGenerator()
    val simpleResolver = new GenericResolver[Int, Char](() => (srg.generate()))

    val s = Seq('a', 'b', 'c', 'd', 'e').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    s.map { rows =>
      simpleResolver.addIds(Seq(rows))
    }

    val x = simpleResolver.getGroups
    assert(x.size == s.size)
  }

  it should "be able to generate a single Label for a Seq[NodeEntity[T]] " in {

    val simpleResolver = new GenericResolver[Int, Char](() => (sng.generate()))

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    simpleResolver.addIds(s)

    val x = simpleResolver.getGroups
    assert(x.size == 1)

  }

  it should "be able to create two separate labels for two separate Seq[NodeEntity[T]]" in {
    val simpleResolver = new GenericResolver[Int, Char](() => (sng.generate()))

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    s.map(entry => simpleResolver.addIds(Seq(entry)))

    val x = simpleResolver.getGroups
    assert(x.size == 2)
  }

  it should "be able to merge two labels that share a common NodeEntity" in {

    val simpleResolver = new GenericResolver[Int, Char](() => (sng.generate()))

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val t = Seq('b', 'c').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    simpleResolver.addIds(s)
    simpleResolver.addIds(t)

    val x = simpleResolver.getGroups

    assert(x.size == 1)
  }

  it should "merge two previously independent labels that both applied to a single NodeEntity" in {

    val simpleResolver = new GenericResolver[Int, Char](() => (sng.generate()))

    val s = Seq('a').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val t = Seq('b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val u = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    simpleResolver.addIds(s)
    simpleResolver.addIds(t)
    simpleResolver.addIds(u)

    val x = simpleResolver.getGroups

    assert(x.size == 1)
  }

  it should "choose the real label when merging multiple labels in a list" in {

    val simpleResolver = new GenericResolver[Int, Char](() => (sng.generate()))

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val t = Seq('b', 'c').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val u = Seq('c', 'd').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    simpleResolver.addIds(s)
    simpleResolver.addIds(u)
    simpleResolver.addIdsWithKnownLabel(t, label1)

    val x = simpleResolver.getGroups

    assert(x.size == 1)
    x.keys.foreach(y => assert(y.identifier == label1))
  }

  it should "choose the real label when merging labels in an unsorted list of pseudo and non-pseudo labels" in {
    val simpleResolver = new GenericResolver[Int, Char](() => sng.generate())

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val t = Seq('c', 'd').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val u = Seq('b', 'c').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val v = Seq('d', 'e').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    simpleResolver.addIds(s)
    simpleResolver.addIds(t)

    simpleResolver.addIdsWithKnownLabel(u, label1)
    simpleResolver.addIds(v)

    val x = simpleResolver.getGroups

    assert(x.size == 1)
    x.keys.foreach(y => assert(y.identifier == label1))
  }

  it should "throw an exception if it finds two labels that map to the same node" in {

    val simpleResolver = new GenericResolver[Int, Char](() => sng.generate())

    val s = Seq('a', 'b').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    val t = Seq('b', 'c').map { entry =>
      NodeEntity(s"Group $entry", entry)
    }

    assertThrows[java.lang.IllegalStateException] {
      simpleResolver.addIdsWithKnownLabel(s, label1)
      simpleResolver.addIdsWithKnownLabel(t, label2)
    }
  }
}
