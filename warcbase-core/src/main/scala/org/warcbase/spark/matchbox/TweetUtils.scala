package org.warcbase.spark.matchbox

import org.json4s.JsonAST._

object TweetUtils {
  implicit class JsonTweet(tweet: JValue) {
    implicit lazy val formats = org.json4s.DefaultFormats

    def id(): String = try { (tweet \ "id_str").extract[String] } catch { case e: Exception => null}
    def createdAt(): String = try { (tweet \ "created_at").extract[String] } catch { case e: Exception => null}
    def text(): String = try { (tweet \ "text").extract[String] } catch { case e: Exception => null}
    def lang: String = try { (tweet \ "lang").extract[String] } catch { case e: Exception => null}

    def username(): String = try { (tweet \ "user" \ "screen_name").extract[String] } catch { case e: Exception => null}
    def isVerifiedUser(): Boolean = try { (tweet \ "user" \ "screen_name").extract[String] == "false" } catch { case e: Exception => false}

    def followerCount: Int = try { (tweet \ "user" \ "followers_count").extract[Int] } catch { case e: Exception => 0}
    def friendCount: Int = try { (tweet \ "user" \ "friends_count").extract[Int] } catch { case e: Exception => 0}
  }
}