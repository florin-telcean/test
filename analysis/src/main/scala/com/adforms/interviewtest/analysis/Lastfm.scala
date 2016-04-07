package com.adforms.interviewtest.analysis

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.adforms.interviewtest.commons._
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * This is the API for solving the test
  * @param logFile user event log file
  * @param userProfilesFile the file with the user profile information
  * @param sparkMaster identifies the Spark cluster
  */
class Lastfm(val logFile: String, val userProfilesFile: String, val sparkMaster: String) {
  // create a Spark configuration with the provided Master
  val conf = new SparkConf().setAppName("Lastfm play around").set("spark.ui.port", "4141").setMaster(sparkMaster)
  // this was changed in the idea of allowing multiple contexts in the web service
  conf.set("spark.driver.allowMultipleContexts", "true")

  val context = new SparkContext(conf)

  // an sql context is required in order to use spark sql
  val sqlContext = new SQLContext(context)

  // hive context is required in order to use window functions
  val hiveContext = new HiveContext(context)

  // schema applied to logentries table
  val logFileSchema = StructType(Array(
    StructField("user", StringType, true),
    StructField("ts", LongType, true),
    StructField("artistId", StringType, true),
    StructField("artist", StringType, true),
    StructField("songId", StringType, true),
    StructField("song", StringType, true),
    StructField("user_id", IntegerType, true),
    StructField("artist_id", IntegerType, true),
    StructField("song_id", IntegerType, true),
    StructField("day_of_week", IntegerType, true),
    StructField("hour_of_day", IntegerType, true)))

  // schema applied to userprofiles table
  val userProfileSchema = StructType(Array(
    StructField("user", StringType, true),
    StructField("gender", StringType, true),
    StructField("age", IntegerType, true),
    StructField("country", StringType, true),
    StructField("registered", LongType, true),
    StructField("user_id", IntegerType, true),
    StructField("gender_id", IntegerType, true),
    StructField("country_id", IntegerType, true)))

  /**
    * RDD of user profile rows(user, gender, age, country, registered, user_id, gender_id, country_id)
    * registered represents the registration time stamp representes as number of milliseconds
    * gender_id: 1=male, 2=female, 0=not specied
    * user_id is the hash code of the user, without sign
    * country_id is the hash code of the country, without sign
  */
  val userProfileRowRDD = context.textFile(userProfilesFile)
    .filter(line => !line.startsWith("#") && line.trim.length > 0)
    .map(line => line.split('\t'))
    .filter(f => f.length == 5)
    .map(f => Row(f(0), f(1),
      NumberUtils.toInt(f(2), 0),
      f(3),
      new SimpleDateFormat("MMM dd, yyyy").parse(f(4).trim).getTime,
      NumberUtils.toInt(f(0).substring(5), 0),
      f(1).toLowerCase() match { case "m" => 1 case "f" => 2 case default => 0 },
      f(3).hashCode & 0x7FFFFFFF ))

  // create user profiles Data frame by applying table schema
  val sqlUserProfileDF = sqlContext.applySchema(userProfileRowRDD, userProfileSchema)
  sqlUserProfileDF.registerTempTable("userprofiles")

  /**
    * RDD of log file rows(user, ts, artistId, artist, songId, song, user_id, artist_id, song_id, day_of_week, hour_of_day)
    * The rows in orginal file are filtered such way that only the ones with defined artistId and songId are preserved. This was done
    */
  val logRowRDD = context.textFile(logFile)
    .map(line => line.split('\t'))
    .filter(fields => fields(2).length > 0 && fields(4).length > 0)
    .map(f => Row(f(0),
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(f(1)).getTime, f(2), f(3), f(4), f(5),
      NumberUtils.toInt(f(0).substring(5), 0),
      f(2).hashCode & 0x7FFFFFFF,
      f(4).hashCode & 0x7FFFFFFF,
      {
        val cal = java.util.Calendar.getInstance()
        cal.setTime(new java.util.Date( (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(f(1)).getTime)))
        cal.get(java.util.Calendar.DAY_OF_WEEK);
      },
      {
        val cal = java.util.Calendar.getInstance()
        cal.setTime(new java.util.Date( (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(f(1)).getTime)))
        cal.get(java.util.Calendar.HOUR_OF_DAY);
      }
    ))


  val sqlLogDF = sqlContext.applySchema(logRowRDD, logFileSchema)
  sqlLogDF.registerTempTable("logentries")

  val hiveLogDF = hiveContext.applySchema(logRowRDD, logFileSchema)
  hiveLogDF.registerTempTable("logentries")

  // Join the log entries with user profiles. it will be used to build different prediction models
  val sqlJoinedLogAndUserDF = hiveLogDF.join(sqlUserProfileDF, Seq("user_id"), "left")

  // the map of distinct artists in the data set
  //  val artistsMap = logRowRDD.map(r => r.get(7) -> (r.get(2), r.get(3))).distinct().collectAsMap()
  val artistsMap = sqlContext.sql("SELECT artist_id, first(artistId), first(artist) FROM logentries GROUP BY artist_id")
    .map(r => r.getAs[Int](0) -> (r.getAs[String](1), r.getAs[String](2)))
    .collectAsMap()

  // the map of distinct songs in the data set
  //  val songsMap = logRowRDD.map(r => r.get(8) -> (r.get(2), r.get(3), r.get(4), r.get(5))).distinct().collectAsMap()

  val songsMap = sqlContext.sql("SELECT song_id, first(artistId), first(artist), first(songId), first(song) FROM logentries GROUP BY song_id")
    .map(r => r.getAs[Int](0) -> (r.getAs[String](1), r.getAs[String](2),  r.getAs[String](3),  r.getAs[String](4)))
    .collectAsMap()


  /**
    * Finds distinct users in the dataset using spark sql
    * @return List of distinct users in alphabetical order
    */
  def uniqueUsers(): Array[String] = {
    val users = sqlContext.sql("SELECT distinct user FROM logentries ORDER BY user")
    val listOfUsers = users.select("user").rdd.map(r => r(0).asInstanceOf[String]).collect()
    return listOfUsers
  }

  /**
    * Finds distinct users in the dataset using rdd api
    * @return List of distinct users in alphabetical order
    */
  def uniqueUsers2(): Array[String] = {
    val users = context.textFile(logFile).map(line => line.split('\t')(0)).distinct().toArray()
    return users
  }

  /**
    * Finds users with highest number of distinct songs played
    * @param N Number of users to return
    * @return List of users and the count of distinct songs played
    */
  def topUsers(N: Integer): Array[TopUser] = {
    val users = sqlContext.sql("SELECT user, count(distinct(songId)) as songCount FROM logentries GROUP BY user ORDER BY songCount desc LIMIT " + N)
    val result = users.rdd.map(row => new TopUser(row.getString(0), row.getLong(1))).collect()
    return result
  }

  /**
    * Finds the top most played songs
    * @param N Number of songs
    * @return List of songs alog with the play count
    */
  def topSongs(N: Integer): Array[TopSong] = {
    val users = sqlContext.sql("SELECT songId, count(*) as playCount FROM logentries GROUP BY songId ORDER BY playCount desc LIMIT " + N)
    val result = users.rdd.map(row => new TopSong(row.getString(0), row.getLong(1))).collect()
    return result
  }

  /**
    * Calculates top user sessions using spark sql and Hive context required for window functions
    * @param N the number of sessions to be returned
    * @return list of sessions with info about length, user, songs
    */
  def topSessions(N: Integer): Array[TopSessionSong] = {
    // generates sessions tabels using 'lag' window function
    val sessions = hiveContext.sql(
      "SELECT *, " +
        " SUM(is_new_session) OVER (PARTITION BY user ORDER BY ts) AS user_session_id " +
      " FROM " +
      " ( " +
        "  SELECT *, (ts - pts) as pdiff, CASE WHEN pts IS NULL OR (ts - pts) > 1200000 THEN 1 ELSE 0 END as is_new_session " +
        "  FROM " +
            " ( " +
            "  SELECT *, lag(ts) OVER (PARTITION BY user ORDER BY ts) as pts " +
            "  FROM logentries " +
            "  ) prev " +
      " ) final "
    )

    sessions.registerTempTable("sessions")

    // top N longest sessions
    val longestSessions = hiveContext.sql("SELECT user, user_session_id, (max(ts) - min(ts)) as session_length FROM sessions GROUP BY user, user_session_id ORDER BY session_length DESC LIMIT " + N)
    longestSessions.registerTempTable("longest_sessions")

    // get session details by joining with the sessions table
    val longestSessionsInfo = hiveContext.sql("SELECT s.user_session_id, ls.session_length, s.user, s.artist, s.song " +
      "FROM sessions s " +
      " INNER JOIN longest_sessions ls " +
      "   ON s.user = ls.user AND s.user_session_id = ls.user_session_id " +
      "ORDER BY ls.session_length DESC, s.user")

    val topSessions = longestSessionsInfo.map(s => new TopSessionSong(s.getLong(0), s.getLong(1), s.getString(2), s.getString(3), s.getString(4))).collect()
    return topSessions
  }

  /**
    * Top sessions implementation using Dataframe api with Hive context required for window functions
    * @param N the number of sessions to be returned
    * @return list of sessions with info about length, user, songs
    */
  def topSessions2(N: Integer): Array[TopSessionSong] = {
    val window = Window.partitionBy("user").orderBy("ts")
    val slag = hiveLogDF.withColumn("pts", lag("ts", 1).over(window))
    val spdiff = slag.withColumn("pdiff", (slag("ts") - slag("pts")))
    val sflag = spdiff.withColumn("is_new_session", when(spdiff("pdiff") > 1200000 or spdiff("pts").isNull, 1).otherwise(0))
    val sessions = sflag.withColumn("user_session_id", sum("is_new_session").over(window))
    val sessionsWithLength = sessions.groupBy("user", "user_session_id").agg((max("ts") - min("ts")).as("session_length"))
    val longestSessions = sessionsWithLength.orderBy(desc("session_length")).limit(N)
    val longestSessionsInfo = sessions.join(longestSessions, Seq("user", "user_session_id"), "inner").orderBy(desc("session_length"), sessions("user"))
    val topSessions = longestSessionsInfo.map(s => new TopSessionSong(s.getAs[Long]("user_session_id"), s.getAs[Long]("session_length"), s.getAs[String]("user"), s.getAs[String]("artist"), s.getAs[String]("song"))).collect()

    return topSessions
  }


  /**
    * Predicts the next song an user will play next time.
    * It trains a Naive Bayes mutinominal classifier where the next song is model as function
    * of previous song and user profile information like country, gender, age
    * @param userName the name of the user
    * @return predicted song
    */
  def predictNextSong(userName: String): Song ={
    // convert user naem to id
    val userId = NumberUtils.toInt(userName.substring(5), 0)
    val modelType = "bayes"
    // build model file name
    val modelFile = "models/model_predict_song_" + modelType + "_" + (logFile.hashCode & 0x7FFFFFFF)
    val modelExists = Files.exists(Paths.get(modelFile))

    // partition the dataset on user and use the window function (lead) to get next played song
    val window = Window.partitionBy(hiveLogDF("user")).orderBy("ts")
    val sleadDF = sqlJoinedLogAndUserDF.withColumn("next_song_id", lead("song_id", 1).over(window))
    val nextSongsDF = sleadDF.where(sleadDF("next_song_id").isNotNull)

    // prepare data used to train the model
    val parsedData = nextSongsDF.map { r =>
      LabeledPoint(r.getAs[Int]("next_song_id").toDouble,
        Vectors.dense(r.getAs[Int]("user_id").toDouble, r.getAs[Int]("song_id").toDouble, r.getAs[Int]("country_id").toDouble,
          r.getAs[Int]("gender_id").toDouble, r.getAs[Int]("age").toDouble))
    }

    // split the data into a training set and a test set
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    // find last user event in the log
    val lastUserEvent = sqlLogDF.where(sqlLogDF("user_id") === userId).orderBy(desc("ts")).take(1)(0)
    // extract last played song
    val lastSongId = lastUserEvent.getAs[Int]("song_id")

    // build or load the model
    val model = modelExists match {
      case true => NaiveBayesModel.load(context, modelFile)
      case false => {
        // train the model
        val newModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
        // check the accuracy of the model

        val predictionAndLabel = test.map(p => (newModel.predict(p.features), p.label))
        val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
        println("Bayes model accuracy = " + accuracy)

        // save the model on the disk to reuse it later
        newModel.save(context, modelFile)
        newModel
      }
    }

    // use the user profile info and last played song to apply the model
    val userDataRow = sqlUserProfileDF.where(sqlUserProfileDF("user_id") === userId).take(1)(0)
    val predictedSongId = model.predict(Vectors.dense(userId, lastSongId.toDouble, userDataRow.getAs[Int]("country_id").toDouble,
      userDataRow.getAs[Int]("gender_id").toDouble, userDataRow.getAs[Int]("age").toDouble)).asInstanceOf[Int]

    // get the song information baes on the rpedicted id
    return new Song(songsMap(predictedSongId)._1.toString, songsMap(predictedSongId)._2.toString, songsMap(predictedSongId)._3.toString, songsMap(predictedSongId)._4.toString)
  }

  /**
    * Builds ALS (alternating least squares) recommender models for artists and songs and return recommended songs and artists for given user
    * The labels are artists or songs and the ratings are calculated as the number of play events in the log for the given artist or song
    * @param userName the name of the user
    * @param noOfRecommendations the number of recommendations to be returned
    * @return list of recommended artists and list of recommended songs
    */
  def recommend (userName: String, noOfRecommendations: Int): Recommendations = {
    // convert user name to id
    val userId = NumberUtils.toInt(userName.substring(5), 0)

    // build the file names for artist and song recommendation models
    val songModelFile = "models/model_recom_song_" + (logFile.hashCode & 0x7FFFFFFF)
    val artistModelFile = "models/model_recom_artist_" + (logFile.hashCode & 0x7FFFFFFF)

    // determine the set of artists and songs already played by the user
    val artistsPlayedAlreadyByUser = sqlContext.sql("SELECT DISTINCT artist_id FROM logentries WHERE user_id = " + userId).collect().map(r => r.get(0).asInstanceOf[Int]).toSet;
    val songsPlayedAlreadyByUser = sqlContext.sql("SELECT DISTINCT song_id FROM logentries WHERE user_id = " + userId).collect().map(r => r.get(0).asInstanceOf[Int]).toSet;

    // determine set of artists and songs played by other users
    val artistsPlayedAlreadyByOthers = sqlContext.sql("SELECT DISTINCT artist_id FROM logentries WHERE user_id != " + userId).collect().map(r => r.get(0).asInstanceOf[Int]).toSet;
    val songsPlayedAlreadyByOthers = sqlContext.sql("SELECT DISTINCT song_id FROM logentries WHERE user_id != " + userId).collect().map(r => r.get(0).asInstanceOf[Int]).toSet;

    // determine artists and songs not played yet by the user
    val artistsNotPlayedYet = artistsPlayedAlreadyByOthers.diff(artistsPlayedAlreadyByUser).toSeq
    val songsNotPlayedYet = songsPlayedAlreadyByOthers.diff(songsPlayedAlreadyByUser).toSeq

    // calculate the ratings in terms of play counts
    val userSongPlayCount = sqlContext.sql("SELECT user_id, song_id, count(*) as song_play_count FROM logentries GROUP BY user_id, song_id")
    val userArtistPlayCount = sqlContext.sql("SELECT user_id, artist_id, count(*) as artist_play_count FROM logentries GROUP BY user_id, artist_id")

    val ratingsSongs = userSongPlayCount
      .map(r => Rating(r.getAs[Integer]("user_id"), r.getAs[Integer]("song_id"), r.getAs[Long]("song_play_count")))
    val ratingsArtists = userArtistPlayCount
      .map(r => Rating(r.getAs[Integer]("user_id"), r.getAs[Integer]("artist_id"), r.getAs[Long]("artist_play_count")))

    // ALS alg parameters
    val rank = 10
    val numIterations = 10

    // train or load the model for song recommendations
    val songModelExists = Files.exists(Paths.get(songModelFile))
    val modelSongRecommendation =  songModelExists match {
      case true => MatrixFactorizationModel.load(context, songModelFile)
      case false => ALS.train(ratingsSongs, rank, numIterations, 0.01)
    }

    // train or load the model for artist recommendations
    val artistModelExists = Files.exists(Paths.get(artistModelFile))
    val modelArtistRecommendation = artistModelExists match {
      case true => MatrixFactorizationModel.load(context, artistModelFile)
      case false => ALS.train(ratingsArtists, rank, numIterations, 0.01)
    }

    // save the models on the disk
    if(!songModelExists)
      modelSongRecommendation.save(context, songModelFile)

    if(!artistModelExists)
      modelArtistRecommendation.save(context, artistModelFile)

    // recommend artists not played yet by the user
    val artistCandidates = context.parallelize[Int](artistsNotPlayedYet).map(t => (userId, t))
    val artistPredictions = modelArtistRecommendation.predict(artistCandidates)
    val recommendedArtistIds = artistPredictions.top(noOfRecommendations)(Ordering.by[Rating,Double](_.rating))

    // recommend songs not played yet by the user
    val songCandidates = context.parallelize[Int](songsNotPlayedYet).map(t => (userId, t))
    val songPredictions = modelSongRecommendation.predict(songCandidates)
    val recommendedSongIds = songPredictions.top(noOfRecommendations)(Ordering.by[Rating,Double](_.rating))

    // extract recommended artist information
    val aa = recommendedArtistIds
      .map(id => {
        val r = artistsMap(id.product)
        new Artist(r._1.toString, r._2.toString)
      })

    // extract recommended songs information
    val ss = recommendedSongIds
      .map(id =>  {
        val r = songsMap(id.product)
        new Song(r._1.toString, r._2.toString, r._3.toString, r._4.toString)
      })

    return new Recommendations(aa, ss)
  }

  /**
    * Predict when a given user will play next song. We model the time interval between 2 consequtive play events as function of day of the week and hour of the day
    * @param userName the name of the user as in the file
    * @return predicted day and time
    */
  def predictNextPlayTime(userName: String): java.util.Date ={
    val userId = NumberUtils.toInt(userName.substring(5), 0)
    val modelType = "bayes" // "linear", "bayes"

    val modelFile = "models/model_predict_time_" + modelType + "_" + (logFile.hashCode & 0x7FFFFFFF)
    val modelExists = Files.exists(Paths.get(modelFile))

    val window = Window.partitionBy("user").orderBy("ts")
    val slag = hiveLogDF.withColumn("pts", lag("ts", 1).over(window))
    val spdiff = slag.withColumn("pdiff", (slag("ts") - slag("pts")))
    val sflag = spdiff.withColumn("is_new_session", when(spdiff("pdiff") > 1200000 or spdiff("pts").isNull, 1).otherwise(0))
    //      .where(spdiff("pdiff") > 60000 && spdiff("user_id") == userId)


    // prepare the data used by the model (user id, day of week, hour of day)
    val parsedData = sflag.map { r =>
      LabeledPoint(r.getAs[Long]("pdiff").toDouble,
        Vectors.dense(r.getAs[Int]("user_id").toDouble, r.getAs[Int]("day_of_week").toDouble, r.getAs[Int]("hour_of_day").toDouble))
    }

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val lastUserEvent = sqlLogDF.where(sqlLogDF("user_id") === userId).orderBy(desc("ts")).take(1)(0)
    val lastTs = lastUserEvent.getAs[Long]("ts")
    val lastTsDayOfWeek = lastUserEvent.getAs[Int]("day_of_week")
    val lastTsHourOfDay = lastUserEvent.getAs[Int]("hour_of_day")


    //    val model = modelExists match {
    //      case true => LinearRegressionModel.load(context, modelFile)
    //      case false => {
    //        val numIterations = 100
    //        val stepSize = 0.00000001
    //        val newModel = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
    //
    //        // Evaluate model on training examples and compute training error
    //        val valuesAndPreds = parsedData.map { point =>
    //          val prediction = newModel.predict(point.features)
    //          (point.label, prediction)
    //        }
    //
    //        val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    //        println("training Mean Squared Error = " + MSE)
    //
    //        newModel.save(context, modelFile)
    //        newModel
    //      }
    //    }
    //    val predictedTime = model.predict(Vectors.dense(userId, lastTsDayOfWeek, lastTsHourOfDay)).asInstanceOf[Long]
    //    return predictedTime

    // load or train the model
    val model = modelExists match {
      case true => NaiveBayesModel.load(context, modelFile)
      case false => {
        val newModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

        val predictionAndLabel = test.map(p => (newModel.predict(p.features), p.label))
        val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
        println("Bayes model accuracy = " + accuracy)

        newModel.save(context, modelFile)
        newModel
      }
    }
    val predictedTime = model.predict(Vectors.dense(userId, lastTsDayOfWeek, lastTsHourOfDay)).asInstanceOf[Long]

    val calendar = java.util.Calendar.getInstance()
    calendar.setTime(new Date(lastTs))
    calendar.add(java.util.Calendar.MILLISECOND, predictedTime.toInt)

    return calendar.getTime
  }
}

