package ru.delimobil

import cats.MonadThrow
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.applicative._
import cats.syntax.option._
import fs2.Stream

import scala.concurrent.duration._

object FS2Example extends IOApp.Simple {

  case class MaybeInt(int: Option[Int])

  private val empty: MaybeInt = MaybeInt(none)

  private val big = 2

  private val small = 1

  private val track = empty.copy(int = big.some)

  def run: IO[Unit] = {
    val singleRun =
      Stream(track, track)
        .repeat
        .through(assemble)
        .evalTap(checkDateValidity)
        .parEvalMap(10)(_ => IO.unit)
        .handleErrorWith(ex => Stream.exec(IO.println(s"Caught ${ex.getMessage}")))
        .groupWithin(512, 1.second)
        .compile
        .drain

    Stream.repeatEval(singleRun).compile.drain.guaranteeCase(IO.println)
  }

  private def assemble(tracks: Stream[IO, MaybeInt]): Stream[IO, MaybeInt] =
    (Stream(empty) ++ tracks)
      .sliding(2)
      .map { chunk => MaybeInt(chunk.head.get.int) }
      .chunks
      .flatMap(chunk => Stream.fromOption(chunk.last))

  private def checkDateValidity(updateAction: MaybeInt): IO[Unit] = {
    val action = MonadThrow[IO].raiseError(new IllegalArgumentException("TheException"))
    action.whenA(updateAction.int.exists(_ > small))
  }
}
