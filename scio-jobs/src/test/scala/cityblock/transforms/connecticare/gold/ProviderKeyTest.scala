package cityblock.transforms.connecticare.gold

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection

class ProviderKeyTest extends PipelineSpec {
  "ProviderKey" should "group correctly" in {
    val left: List[(ProviderKey, Int)] = List(
      (ProviderKey("a"), 1),
      (ProviderKey("a"), 2),
      (ProviderKey("b"), 1),
      (ProviderKey("c"), 2),
    )

    val right: List[(ProviderKey, String)] = List(
      (ProviderKey("a"), "1"),
      (ProviderKey("a"), "2"),
      (ProviderKey("b"), "1"),
    )

    // if we don't specify the type of an empty set, we'll break `should`'s type inference
    val expected = Iterable(
      (ProviderKey("a"), (Set(1, 2), Set("1", "2"))),
      (ProviderKey("b"), (Set(1), Set("1"))),
      (ProviderKey("c"), (Set(2), Set[String]()))
    )

    runWithContext { sc =>
      val l = sc.parallelize(left)
      val r = sc.parallelize(right)

      val result: SCollection[(ProviderKey, (Set[Int], Set[String]))] =
        l.cogroup(r)
          .mapValues {
            // convert to Sets because containInAnyOrder doesn't ignore order of Iterables within
            // tuples
            case (lefts, rights) => (lefts.toSet, rights.toSet)
          }

      result should containInAnyOrder(expected)
    }
  }
}
