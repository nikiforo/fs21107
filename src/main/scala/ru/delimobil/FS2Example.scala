package ru.delimobil

import cats.MonadThrow
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.applicative._
import cats.syntax.option._
import fs2.Stream

import scala.concurrent.duration._

object FakeDP {

  case class MaybeZDT(zdt: Option[Int])

  val empty: MaybeZDT = MaybeZDT(none)

  val big = 2

  val small = 1

  def make =
    transferTracks
      .handleErrorWith(ex => Stream.exec(IO.println(s"Caught ${ex.getMessage}")))
      .groupWithin(512, 1.second)
      .compile
      .drain

  def transferTracks: Stream[IO, Int] = {
    val track = empty.copy(zdt = big.some)
    Stream(track, track)
      .repeat
      .through(assemble)
      .evalTap(checkDateValidity)
      .parEvalMap(10)(_ => IO.pure(1))
  }

  private def assemble(tracks: Stream[IO, MaybeZDT]): Stream[IO, MaybeZDT] =
    (Stream(empty) ++ tracks)
      .sliding(2)
      .map { chunk => MaybeZDT(chunk.head.get.zdt) }
      .chunks
      .flatMap(chunk => Stream.fromOption(chunk.last).map(action => MaybeZDT(action.zdt)))

  private def checkDateValidity(updateAction: MaybeZDT): IO[Unit] =
    MonadThrow[IO].raiseError(new IllegalArgumentException("TheException")).whenA(updateAction.zdt.exists(_ > small))
}

object FS2Example extends IOApp.Simple {
  def run: IO[Unit] = Stream.repeatEval(FakeDP.make).compile.drain
}
