package cityblock.utilities

import java.util.NoSuchElementException

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext, ScioResult}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.PipelineOptions

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ScioUtils extends Loggable {

  /**
   * Run the [[com.spotify.scio]] pipeline specified by `fn` as a separate Dataflow job.
   *
   * [[runJob]] submits the job to Dataflow but does not wait for the job to finish, allowing
   * multiple jobs to be submitted in parallel.
   *
   * {{{
   *   val args: Array[String] = ...
   *   val (_, xs) = runJob("make-x", args) { sc =>
   *      sc.doSomeIO()
   *        .map(makeX)
   *   }
   *   val (_, ys) = runJob("make-y", args) { sc =>
   *      xs.waitForResult()
   *        .open(sc)
   *        .map(makeY)
   *   }
   * }}}
   *
   * WARNING: When executing this by iterating over a sequence to create a sequence of
   * parallel jobs, make sure that the iterator you're using is not lazy. For example, List.map
   * will block while waiting for each job execution, while Array.map will correctly create a
   * parallel job per element.
   *
   * @param jobName the name of the job as it should appear in the Dataflow UI
   * @param args command-line args specifying [[com.spotify.scio]] and application options
   * @param fn the [[com.spotify.scio]] pipeline to run
   * @tparam T output of the job (if any). [[T]] should be either a `Future[Tap[A]]` or a set of
   *           `Future[Tap[A]]`s.
   * @return a [[ScioResult]] to monitor the job, and futures to wait on the job's outputs
   */
  def runJob[T](
    jobName: String,
    args: Array[String]
  )(
    fn: ScioContext => T
  )(
    implicit environment: Environment
  ): (ScioResult, T) = {
    val (sc, _) = ContextAndArgs(args)
    sc.setJobName(s"$jobName-${environment.jobId}")

    val t = fn(sc)
    val result = sc.close()

    (result, t)
  }

  def maybeRunJob[T](
    jobName: String,
    args: Array[String],
    condition: Boolean
  )(fn: ScioContext => T)(implicit environment: Environment): (Option[ScioResult], Option[T]) =
    if (condition) {
      val (result, value) = ScioUtils.runJob(jobName, args)(fn)
      (Some(result), Some(value))
    } else {
      (None, None)
    }

  /**
   * Run the [[com.spotify.scio]] pipeline specified by `fn` as a separate Dataflow job.
   *
   * [[runJob]] submits the job to Dataflow but does not wait for the job to finish, allowing
   * multiple jobs to be submitted in parallel.
   *
   * Note that setting the job name on a [[ScioContext]] constructed from
   * `opts` modifies `opts`. [[runJob]] resets `opts`' before returning, so it will not modify the
   * name of other jobs created with `opts`.
   *
   * {{{
   *   val opts = sc.optionsAs[DataflowPipelineOptions] // sc is open
   *   val (_, xs) = runJob("make-x", opts) { sc =>
   *      sc.doSomeIO()
   *        .map(makeX)
   *   }
   *   val (_, ys) = runJob("make-y", opts) { sc =>
   *      xs.waitForResult()
   *        .open(sc)
   *        .map(makeY)
   *   }
   * }}}
   *
   *
   * WARNING: When executing this by iterating over a sequence to create a sequence of
   * parallel jobs, make sure that the iterator you're using is not lazy. For example, List.map
   * will block while waiting for each job execution, while Array.map will correctly create a
   * parallel job per element.
   *
   * @param jobName the name of the job as it should appear in the Dataflow UI
   * @param opts [[PipelineOptions]] for the job
   * @param fn the [[com.spotify.scio]] pipeline to run
   * @tparam T output of the job (if any). [[T]] should be either a `Future[Tap[A]]` or a set of
   *           `Future[Tap[A]]`s.
   * @tparam O subtype of [[PipelineOptions]]
   * @return a [[ScioResult]] to monitor the job, and futures to wait on the job's outputs
   */
  def runJob[T, O <: PipelineOptions](
    jobName: String,
    opts: O
  )(
    fn: ScioContext => T
  )(
    implicit environment: Environment
  ): (ScioResult, T) = {
    // TODO see if we can duplicate opts
    val oldName = opts.getJobName
    val sc = ScioContext(opts)
    sc.setJobName(s"$jobName-${environment.jobId}")

    val t = fn(sc)
    val result = sc.close()
    opts.setJobName(oldName)

    (result, t)
  }

  /**
   * Wait for `future` to complete and open the resulting [[Tap]] in `sc`.
   *
   * {{{
   *   val args: Array[String] = ...
   *   val (_, xs) = runJob("make-x", args) { sc =>
   *      sc.doSomeIO()
   *        .map(makeX)
   *   }
   *   val (_, ys) = runJob("make-y", args) { sc =>
   *      waitAndOpen(xs, sc).map(makeY)
   *   }
   *}}}
   *
   * @param future future to wait on. This should be produced from a [[com.spotify.scio]] IO
   *               operation or a call to [[com.spotify.scio.values.SCollection.materialize]].
   * @param sc the [[ScioContext]] in which to open the [[Tap]]. Must be open.
   * @param duration optional wait timeout. Defaults to [[Duration.Inf]].
   * @tparam T the type of the collection contained within `future`
   * @return an collection of [[T]] that is operable in `sc`
   */
  def waitAndOpen[T: Coder](future: Future[Tap[T]], sc: ScioContext)(
    implicit duration: Duration = Duration.Inf): SCollection[T] =
    Try(future.waitForResult(duration).open(sc)) match {
      case Success(xs) => xs
      case Failure(e) =>
        throw if (sc.isTest &&
                  e.isInstanceOf[NoSuchElementException] &&
                  e.getStackTrace.exists(_.getClassName.contains("InMemorySink"))) {
          new NoSuchElementException(
            e.getMessage +
              """
               |This probably means that you've materialized an empty
               |SCollection. Make sure your test is supplying non-empty
               |collections for each input, and check that your job's
               |intermediate steps aren't producing empty output.
              """.stripMargin)
        } else {
          e
        }
    }

  /**
   * Returns a `SCollection` for a given function that takes in a ScioContext (as well as
   * additional arguments if specified).
   *
   * A standalone Dataflow job is spawned that does what `f` specifies, after which the primary
   * artifact, an [[SCollection]], is then passed onto to the caller [[ScioContext]] as it is
   * materialized in temporary storage.
   *
   *{{{
   *   val xs: SCollection[String] = runWaitOpen("make-x", opts, sc) { innerCtx =>
   *    val raw = innerCtx.doSomeIO()
   *    raw.map(someTransform)
   *   }
   *   // use xs however you want in the context of sc
   *}}}
   *
   * @param name           name of Dataflow job to be spun off and run
   * @param opts           pipeline options (usually passed from existing scio context)
   * @param outerContext   Scio context that waits for the materialized [[SCollection]] produced
   *                       from `f`
   * @param f              function that takes in a Scio context (constructs using opts) that
   *                       returns a [[SCollection]]
   * @tparam T             type contained within the [[SCollection]]
   */
  def runWaitOpen[T: Coder](
    name: String,
    opts: DataflowPipelineOptions,
    outerContext: ScioContext
  )(
    f: ScioContext => SCollection[T]
  )(
    implicit environment: Environment
  ): SCollection[T] = {
    val (_, futureTap) = runJob(name, opts)(f(_).materialize)
    waitAndOpen(futureTap, outerContext)
  }

  /**
   * This trait is used to implement [[cityblock.transforms.Transform.Chainable]].
   *
   * [[Taps]] groups a set of [[com.spotify.scio.io.Tap]]s for easy distribution amongst
   * [[ScioContext]]s.
   *
   * Given case class like
   *
   * {{{
   *   case class Collections(
   *     foo: SCollection[Foo],
   *     bar: Scollection[Bar]
   * }}}
   *
   * implement [[Taps]] like
   *
   * {{{
   *   object Collections {
   *       case class Taps(
   *         foo: Tap[Foo],
   *         bar: Tap[Bar]
   *       )
   *   }
   * }}}
   */
  trait Taps

  /**
   * This trait is used to implement [[cityblock.transforms.Transform.Chainable]].
   *
   * [[Futures]] groups a set of [[scala.concurrent.Future]]s containing
   * [[com.spotify.scio.io.Tap]]s to simplify waiting on all said futures at once.
   *
   * Given case class like
   *
   * {{{
   *   case class Collections(
   *     foo: SCollection[Foo],
   *     bar: Scollection[Bar]
   * }}}
   *
   * implement [[Futures]] like
   *
   * {{{
   *   object Collections {
   *       case class Futures(
   *         foo: Future[Tap[Foo]],
   *         bar: Future[Tap[Bar]]
   *       )
   *   }
   * }}}
   */
  trait Futures {

    /**
     * Wait for `duration` for every [[scala.concurrent.Future]] in this case class to complete.
     *
     * @param duration option timeout. Defaults to [[Duration.Inf]].
     * @return a collection of [[com.spotify.scio.io.Tap]]
     */
    def wait(duration: Duration = Duration.Inf): Taps
  }
}
