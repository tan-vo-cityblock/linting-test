package cityblock.ehrmodels.elation.service

import cityblock.ehrmodels.elation.service.Request.getRequest
import cityblock.utilities.backend.ApiError
import io.circe.Decoder
import com.softwaremill.sttp.{Id, SttpBackend, Uri, UriInterpolator}
import scala.annotation.tailrec

/*
 * This class is only accessible to the `cityblock.ehrmodels.elation.datamodel` package and is meant to handle and resolve all errors encountered
 * This class uses the Request class in order to make get and post requests
 * */

private[elation] object Elation {

  def get[T: Decoder](
    url: Uri
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, T] =
    getRequest[T](url)

  /**
   * This method receives a paginated result of all the entries available in Elation of type T and
   * via tail recursive calls, flattens the pagination to a single list object.
   *
   * NOTE: this method also returns entries that have been soft deleted by Elation. If a patient is
   * deleted and then recreated, it is possible to find duplicates. If you wish to remove such entries
   * filter by the deletion_date column.
   */
  def getAll[T: Decoder](
    url: Uri
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, List[T]] = {

    type Response = Either[ApiError, List[T]]

    @tailrec
    def recurse(nextUrl: Uri, accum: Response): Response = {

      val response = getRequest[ElationList[T]](nextUrl)

      response match {
        case Left(error) => Left(error)
        case Right(pagination) =>
          if (pagination.next.isEmpty) {
            Right(accum.right.get ::: pagination.results)
          } else {
            val nextUri =
              UriInterpolator.interpolate(StringContext(pagination.next.get))
            recurse(nextUri, Right(accum.right.get ::: pagination.results))
          }
      }
    }
    recurse(url, Right(List[T]()))
  }
}
