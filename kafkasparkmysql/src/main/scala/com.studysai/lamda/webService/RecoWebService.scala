package com.studysai.lamda.webService

import java.util
import java.util._
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, PathParam, Produces}

import com.studysai.lamda.proto.Spark.{ItemList, ItemSimilarities, ItemSimilarity}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
  * Created by lj on 2017/5/18.
  * message ItemSimilarities {
        repeated ItemSimilarity itemSimilarites = 1;
    }

    message ItemSimilarity {
        optional int64 itemId = 1;
        optional double similarity = 2;
    }

    message ItemList {
        repeated int64 itemIds = 1;
    }
  */
@Path("/ws/reco")
class RecoWebService {

  private[lamda] val jedis : Jedis = null

  @GET
  @Path("/{userId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getRecoItem(@PathParam("userid") userId : String) : RecoItem = {
    val recoItem : RecoItem = new RecoItem
    val jedis = RedisClient.pool.getResource

    val key : String = String.format("UI:%s", userId)
    val value : String = jedis.get(key)
    if(value == null || value.length <= 0)
      return recoItem

    //list<"II:" + itemIds>
    val userItems = ItemList.parseFrom(value.getBytes())
    val userItemSet = new TreeSet(userItems.getItemIdsList)
    val userItemStrs = userItems.getItemIdsList.map("II:" + _)

    //返回数组，指定键的值列表。, _*:child ++ newChild : _* expands Seq[Node] to Node*
    // (tells the compiler that we're rather working with a varargs, than a sequence). Particularly useful for the methods that can accept only varargs.
    val similarItems: List[String] = jedis.mget(userItemStrs:_*)
    val similarItemSet : Set[ItemSimilarity] = new util.TreeSet[ItemSimilarity]
    for (item <- similarItems) {
      val result = ItemSimilarities.parseFrom(item.getBytes())
      similarItemSet.addAll(result.getItemSimilaritesList)
    }
    val itemIds = similarItemSet.filter(item => !userItemSet.contains(item.getItemId)).take(10).map(_.getItemId)
    recoItem.setItem(itemIds.toArray)
    return recoItem
  }
}
