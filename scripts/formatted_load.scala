import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._




  //Extracting batch number
  val batch_id = sc.textFile("/home/acadgild/project/logs/current-batch.txt").map(x=>x.toInt).toDF().first.getInt(0)

  //Migrating Enriched Data to DataFrame
  val enr_data = sc.textFile("/home/acadgild/project/enriched_data/000000_0")

  val enr_schema = StructType(Array(StructField("User_id",StringType,true),StructField("Song_id",StringType,true),StructField("Artist_id",StringType,true),StructField("u_Timestamp",StringType,true),StructField("Start_ts",StringType,true),
    StructField("End_ts",StringType,true),
    StructField("Geo_cd",StringType,true),
    StructField("Station_id",StringType,true),
    StructField("Song_end_type",IntegerType,true),
    StructField("u_Like",IntegerType,true),
    StructField("Dislike",IntegerType,true),
    StructField("BatchID",IntegerType,true),
    StructField("status",StringType,true)
  ))

  val enr_row_rdd = enr_data.map(_.split(",")).map(r => Row(r(0),r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8).toInt,r(9).toInt,r(10).toInt,r(11).toInt,r(12)))

  val music_data = spark.createDataFrame(enr_row_rdd,enr_schema )
  music_data.createOrReplaceTempView("music_data")

  //Subscribed user
  val subs_data = sc.textFile("/home/acadgild/project/subscribed_users/000000_1")

  val subs_schema = StructType(Array(StructField("User_id",StringType,true),
    StructField("start_dt",StringType,true),
    StructField("end_dt",StringType,true)
  ))

  val subs_row_rdd = subs_data.map(_.split(",")).map(r => Row(r(0),r(1),r(2)))

  val subs_usr_data = spark.createDataFrame(subs_row_rdd,subs_schema )
  subs_usr_data.createOrReplaceTempView("subscribed_user")

  //
  val usr_data = sc.textFile("/home/acadgild/project/user_artists/000000_0")

  val usr_schema = StructType(Array(StructField("User_id",StringType,true),
    StructField("artists",StringType,true)
  ))

  val usr_row_rdd = enr_data.map(_.split(",")).map(r => Row(r(0),r(1)))

  val usr_artist_data = spark.createDataFrame(usr_row_rdd,usr_schema )
  usr_artist_data.createOrReplaceTempView("users_artists")

  //***********************************************************************
  //                             PROBLEM 1
  //***********************************************************************

  val prob_1  = spark.sql("select station_id, count(distinct song_id) Distinct_Songs," +
    " count(distinct user_id) Distinct_users, batchid from music_data " +
    s"where status='fail' and batchid = $batch_id " +
    s"and u_like=1 group by station_id,batchid order by Distinct_Songs " +
    s"DESC limit 10").toDF("station_id","Distinct_Songs","Distinct_users","batchid")

    prob_1.write.format("csv").option("header","true").save(s"/home/acadgild/project/output/top_10_station/batch_$batch_id")

  //***********************************************************************
  //                             PROBLEM 2
  //***********************************************************************

  val prob_2 = spark.sql("select CASE WHEN ( subusr.user_id is null or " +
    "cast(music.u_timestamp as decimal(20,0)) > cast(subusr.end_dt as decimal(20,0)) " +
    ") then 'UNSUBSCRIBED' WHEN (subusr.user_id is NOT null or " +
    "cast(music.u_timestamp as decimal(20,0)) <= cast(subusr.end_dt as decimal(20,0)) " +
    ") then 'SUBSCRIBED' END AS USER_TYPE, " +
    "SUM(ABS(CAST(music.end_ts as decimal(20,0)) - CAST(music.start_ts as decimal(20,0))))" +
    " as duration ,batchid" +
    " from music_data music left outer join " +
    "subscribed_user subusr on music.user_id = subusr.user_id " +
    s"where music.status = 'pass' and music.batchid = $batch_id " +
    s"group by music.batchid, CASE WHEN ( subusr.user_id is null or " +
    "cast(music.u_timestamp as decimal(20,0)) > cast(subusr.end_dt as decimal(20,0)) " +
    ") then 'UNSUBSCRIBED' WHEN (subusr.user_id is NOT null or " +
    "cast(music.u_timestamp as decimal(20,0)) <= cast(subusr.end_dt as decimal(20,0)) " +
    ") then 'SUBSCRIBED' END " ).toDF()

  prob_2.repartition(1).write.format("csv").option("header","true").save(s"/home/acadgild/project/output/user_response/batch_$batch_id")

  //***********************************************************************
  //                             PROBLEM 3
  //***********************************************************************

  val prob_3 = spark.sql("select ua.artists, count(distinct ua.user_id) as count_of_users, " +
    "md.batchid from users_artists ua inner join (select artist_id,song_id,user_id,batchid" +
    s" from music_data where status = 'pass' and batchid = $batch_id)md " +
    s" on ua.artists = md.artist_id and ua.user_id = md.user_id group by ua.artists," +
    s" batchid order by count_of_users DESC limit 10").toDF("user_id","count_of_users","batchid")

    prob_3.write.format("csv").option("header","true").save(s"/home/acadgild/project/output/connected_artist/batch_$batch_id")

  //***********************************************************************
  //                             PROBLEM 4
  //***********************************************************************

  val prob_4 = spark.sql("select song_id, " +
    "SUM(ABS(CAST(end_ts as decimal(20,0)) - " +
    " CAST(start_ts as decimal(20,0)))) as duration,batchid" +
    s" from music_data where status = 'pass' and batchid = $batch_id " +
    "and (u_like= 1 or song_end_type = 0) group by song_id, batchid " +
    "order by duration desc limit 10").toDF("song_id","duration","batch_id")

  prob_4.write.format("csv").option("header","true").save(s"/home/acadgild/project/output/royalty_songs/batch_$batch_id")

