package org.warcbase.util

import org.json4s.JsonAST._

object TweetJsonFunctions {
  implicit class JValueFunctions(tweet: JValue) {
    implicit lazy val formats = org.json4s.DefaultFormats

    def id(): String = (tweet \ "id_str").extract[String]
    def createdAt(): String = (tweet \ "created_at").extract[String]
    def lang: String = (tweet \ "lang").extract[String]

    def username(): String = (tweet \ "user" \ "screen_name").extract[String]
    def isVerifiedUser(): Boolean = (tweet \ "user" \ "screen_name").extract[String] == "false"

    def followerCount: Int = (tweet \ "user" \ "followers_count").extract[Int]
    def friendCount: Int = (tweet \ "user" \ "friends_count").extract[Int]
  }
}