
package com.mycomp

import com.twitter.scalding._
import scala.io.Source
import java.net.URL

import com.twitter.scalding.typed.{TypedSink, CoGroupable, CoGrouped}

case class Book(title: String, author:String)
case class ExtendedBook(title:String, author:String, copies:Long)

class DomainCounter (args: Args) extends Job(args) {
    val library1 = TypedPipe.from(Seq(
    Book("To Kill a Mockingbird", "Harper Lee"),
  Book("The Fountainhead", "Ayn Rand"),
  Book("Atlas Shrugged", "Ayn Rand"),
  Book("A Separate Peace", "John Knowles")
    ))
   val library2 = TypedPipe.from(Seq(
   ExtendedBook("To Kill a Mockingbird", "Harper Lee", 10),
  ExtendedBook("The Fountainhead", "Ayn Rand", 6),
  ExtendedBook("Go Set a Watchman", "Harper Lee", 2)
   ))
    val group1 = library1.groupBy(_.title)
    val group2 = library2.groupBy(_.title)
    val theJoin = group1.join(group2)
    theJoin.write(TypedCsv("/tmp/output"))
    TypedPipe.from(TextLine(args("input")))
        .map { line => new java.net.URL(line).getHost() }
        .groupBy { host => host}
        .size
        .filter{ case (host, cnt) => cnt > 5 }
        .write(TypedCsv(args("output")))

  }

object DomainCounter {

    def main(args: Array[String]): Unit = {
          val progargs = List(
          "-Dmapred.",
          "com.mycomp.DomainCounter",
          "--input", "/home/indix/explore/domainNameCounter/sample.txt",
          "--output", "explore/output",
          "--local").toArray
       Tool.main(progargs)
    }
}
