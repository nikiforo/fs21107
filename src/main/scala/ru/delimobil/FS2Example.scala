package ru.delimobil

import cats.MonadThrow
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.applicative._
import cats.syntax.option._
import fs2.Stream

import scala.concurrent.duration._

object FakeDP {

  case class MaybeInt(int: Option[Int])

  val empty: MaybeInt = MaybeInt(none)

  val big = 2

  val small = 1

  def make =
    transferTracks
      .handleErrorWith(ex => Stream.exec(IO.println(s"Caught ${ex.getMessage}")))
      .groupWithin(512, 1.second)
      .compile
      .drain

  def transferTracks: Stream[IO, Int] = {
    val track = empty.copy(int = big.some)
    Stream(track, track)
      .repeat
      .through(assemble)
      .evalTap(checkDateValidity)
      .parEvalMap(10)(_ => IO.pure(1))
  }

  private def assemble(tracks: Stream[IO, MaybeInt]): Stream[IO, MaybeInt] =
    (Stream(empty) ++ tracks)
      .sliding(2)
      .map { chunk => MaybeInt(chunk.head.get.int) }
      .chunks
      .flatMap(chunk => Stream.fromOption(chunk.last))

  private def checkDateValidity(updateAction: MaybeInt): IO[Unit] =
    MonadThrow[IO].raiseError(new IllegalArgumentException("TheException")).whenA(updateAction.int.exists(_ > small))
}

object FS2Example extends IOApp.Simple {
  def run: IO[Unit] = Stream.repeatEval(FakeDP.make).compile.drain
}
