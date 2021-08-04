package cityblock.utilities

import org.slf4j.{Logger, LoggerFactory}

/**
 * [[Loggable]] does not have to extend [[Serializable]]. When serializing pipeline functions
 * (e.g., the functions passed to map, filter, etc.), Scio applies
 * [[com.spotify.scio.util.ClosureCleaner]] to manage the function's references to outer scopes.
 * Via [[com.spotify.scio.util.ClosureCleaner]], [[Loggable]]'s [[Logger]] is copied and packaged
 * with the associated function.
 */
trait Loggable {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
