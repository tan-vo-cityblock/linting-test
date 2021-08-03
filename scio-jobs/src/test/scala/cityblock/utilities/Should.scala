package cityblock.utilities

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection

object Should extends PipelineSpec {
  def satisfySingle[T: Coder](xs: SCollection[T])(check: T => Boolean): Unit =
    xs should satisfySingleValue[T](check)

  def satisfyIterable[T: Coder](
    xs: SCollection[T]
  )(
    check: Iterable[T] => Boolean
  ): Unit =
    xs should satisfy(check)
}
