package cityblock.member.resolution

object SimpleGenerator {

  class SimpleNumberGenerator {
    private var current = 0

    def generate(): Int =
      this.synchronized {
        current += 1
        this.current
      }
  }

  class SimpleRandomGenerator {
    private val r = scala.util.Random
    private val RANDOM_INT_LIMIT = 5

    def generate(): Int = r.nextInt(RANDOM_INT_LIMIT)
  }
}
