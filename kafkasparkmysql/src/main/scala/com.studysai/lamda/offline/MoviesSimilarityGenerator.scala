package com.studysai.lamda.offline

import com.studysai.lamda.proto.Spark.{ItemList, ItemSimilarities, ItemSimilarity}
import com.studysai.lamda.webService.RedisClient
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lj on 2017/5/22.
  * 电影相似度
  */
object MoviesSimilarityGenerator {

  def main(args: Array[String]): Unit = {
    val dataPath = "data/ml-1m/ratings.dat"

    val conf = new SparkConf().setAppName("moviesAnalyzer").setMaster("local")
    val sc = new SparkContext(conf)

    //临界值
    val threshold = 0.1

    val rows = sc.textFile(dataPath).map(_.split("::")).map(
      p =>
        (p(0).toLong, p(1).toInt, p(2).toDouble)
    )
    val maxMovieId = rows.map(_._2).max() + 1

    val rowRdd = rows.map(
      p =>
        (p._1, (p._2, p._3))
    ).groupByKey().map(
      kv =>
        // 创建一个稀疏本地向量
        Vectors.sparse(maxMovieId, kv._2.toSeq)
    ).cache()

    //读入行矩阵
    val mat = new RowMatrix(rowRdd)
    println(s"mat row/num : ${mat.numRows()}, ${mat.numCols()}")

    //计算列之间的相似度
    val similarities = mat.columnSimilarities(threshold)

    //保存电影间的相似度到redis
    similarities.entries.map{
      case MatrixEntry(i, j, u) => (i, (j, u))
    }.groupByKey(2).map(
      kv =>
        (kv._1, kv._2.toSeq.sortWith(_._2 > _._2).take(20).toMap)
    ).mapPartitions{
      iter =>
      val jedis = RedisClient.pool.getResource
      iter.map{
        case (i, j) =>
          val key = "II:%d".format(i)
          val builder = ItemSimilarities.newBuilder()
          j.foreach{
            item =>
              val itemSimilarity = ItemSimilarity.newBuilder().setItemId(item._1).setSimilarity(item._2)
              builder.addItemSimilarites(itemSimilarity)
          }
          val value = builder.build()
          println(s"key:${key}, value:${value.toString}")
          jedis.set(key, new String(value.toByteArray))
      }
    }.count()

    //保存用户-电影间相似度到redis
    rows.map{
      case (i, j, k) =>
        (i, j)
    }.groupByKey(2).mapPartitions{
      item =>
      val jedis = RedisClient.pool.getResource
      item.map{
        case (i, j ) =>
          val key = ("UI:%d").format(i)
          val builder = ItemList.newBuilder()
          j.foreach{
            id =>
              builder.addItemIds(id)
          }
          val value = builder.build()
          println(s"key:${key}, value:${value.toString}")
          jedis.append(key, new String(value.toByteArray))
      }
    }.count()

    sc.stop()
  }
}

