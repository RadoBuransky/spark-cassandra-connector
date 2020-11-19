package com.datastax.spark.connector.util

import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.partitioner.CqlTokenRange
import com.datastax.spark.connector.rdd.partitioner.dht.Token

private[connector] class ResilientIterator[R](source: Iterator[R],
                                              handler: PartialFunction[ResilientIteratorException, Option[R]],
                                              context: ResilientIteratorContext[_, _])
  extends Iterator[Option[R]] {
  private var failed: Boolean = false

  override def hasNext: Boolean = !failed && source.hasNext
  override def next(): Option[R] = {
    try {
      Some(source.next())
    }
    catch {
      case ex: Exception =>
        failed = true
        handler.applyOrElse(new ResilientIteratorException(context, ex), (_: ResilientIteratorException) => None)
    }
  }
}

private[connector] object ResilientIterator {
  def apply[R](source: Iterator[R],
               handler: PartialFunction[ResilientIteratorException, Option[R]],
               context: ResilientIteratorContext[_, _]): Iterator[R] =
    new ResilientIterator(source, handler, context).flatten
}

private[connector] class ResilientIteratorException(val context: ResilientIteratorContext[_, _], cause: Throwable)
  extends RuntimeException(s"${context.queryParts}, ${context.range}", cause)

private[connector] case class ResilientIteratorContext[V, T <: Token[V]](queryParts: CqlQueryParts,
                                                                         range: CqlTokenRange[V, T])