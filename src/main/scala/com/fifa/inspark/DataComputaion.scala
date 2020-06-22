package com.fifa.inspark

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DataComputaion {
  def computeData(spark:SparkSession,inputFile:String): Unit ={
    spark.sparkContext.setLogLevel("ERROR")
    val fifa2019 = spark.read.option("header","true").option("inferSchema","true").csv("data/data.csv")

    val df = fifa2019.drop("_c0").filter(fifa2019("Club").isNotNull).persist()
    val df1 = df.select("Club","Value","Wage","Preferred Foot","Position","Age")

    //Which club has the most number of left footed midfielders under 30 years of age?
    val age_below_30 =df1.filter(df("Age").cast("Int") < 30 && df("Preferred Foot") === "Left" && df("Position").endsWith("M"))
      .groupBy("Club")
      .agg(count(df("Age")).as("age_count"))
      .orderBy(desc("age_count"),asc("Club"))
    age_below_30.createOrReplaceTempView("AgeCount")
    val age_below_30_count =spark.sql("select * from AgeCount where age_count =(select max(age_count) from AgeCount) ")

    //Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?
    val exp_squad = df1.withColumn("Value",
         when(col("Value").endsWith("M"),regexp_extract(col("Value"),"\\d+",0)*1000000)
        .when(col("Value").endsWith("K"),regexp_extract(col("Value"),"\\d+",0)*1000)
    ).withColumn("Wage",when(col("Wage").endsWith("K"),regexp_extract(col("Wage"),"\\d+",0)*1000))

    val exp_squad_cast=exp_squad.select(col("Club"),col("Preferred Foot"),col("Position"),col("Age"),
      col("Value").cast(IntegerType).as("Value"),
      col("Wage").cast(IntegerType).as("Wage"))

    val exp_squad_final=exp_squad_cast.groupBy("Club")
      .agg(sum("Value").as("sum_val"),sum("Wage").as("sum_wage"))
      .orderBy(desc("sum_val"),desc("sum_wage"))

    //The strongest team by overall rating for a 4-4-2 formation
    df.createOrReplaceTempView("test")
    val defsv ="SELECT Club,Nationality,sum(Overall) as rating from test where Position in ('LDM','LB','LCB','CB','CDM','RB','RCB','RWB') group by Nationality,Club order by rating desc limit 4"
    val defensive_players = spark.sql(defsv)

    val midfldrs = "SELECT Club,Nationality, sum(Overall) as rating from test where Position in ('LCM','LMF','CM','CMF','RCM','RMF','RWM','RM') group by Nationality,Club order by rating desc limit 4"
    val midfielders = spark.sql(midfldrs)

    val strkrs = "SELECT Club,Nationality, sum(Overall) as rating from test where Position in ('CF','RF','ST','LF') group by Nationality,Club order by rating desc limit 2"
    val strikers = spark.sql(strkrs)

    val gk  = "SELECT Club,Nationality, sum(Overall) as rating from test where Position in ('GK') group by Nationality,Club order by rating desc limit 1"
    val goal_keeper = spark.sql(gk)

    val best_team = defensive_players.union(midfielders).union(strikers).union(goal_keeper).orderBy(desc("rating"))

    //Which position pays the highest wage in average?
    val pos_pay = exp_squad_cast.groupBy("Position").agg(avg("Wage")).orderBy(desc("avg(Wage)"))

    val gk_cols = df.select("Name","Overall","Position","Crossing","Finishing","HeadingAccuracy","ShortPassing","Volleys","Dribbling"
      ,"Curve","FKAccuracy","LongPassing","BallControl","Acceleration","SprintSpeed","Agility","Reactions","Balance","ShotPower"
      ,"Jumping","Stamina","Strength","LongShots","Aggression","Interceptions","Positioning","Vision","Penalties","Composure"
      ,"Marking","StandingTackle","SlidingTackle","GKDiving","GKHandling","GKKicking","GKPositioning","GKReflexes")
      .filter(col("Position")==="GK")

    val avg_atts= gk_cols.agg(avg("Crossing").as("Crossing_avg")
      ,avg("Finishing").as("Finishing_avg")
      ,avg("HeadingAccuracy").as("HeadingAccuracy_avg")
      ,avg("ShortPassing").as("ShortPassing_avg")
      ,avg("Volleys").as("Volleys_avg")
      ,avg("Dribbling").as("Dribbling_avg")
      ,avg("Curve").as("Curve_avg")
      ,avg("FKAccuracy").as("FKAccuracy_avg")
      ,avg("LongPassing").as("LongPassing_avg")
      ,avg("BallControl").as("BallControl_avg")
      ,avg("Acceleration").as("Acceleration_avg")
      ,avg("SprintSpeed").as("SprintSpeed_avg")
      ,avg("Agility").as("Agility_avg")
      ,avg("Reactions").as("Reactions_avg")
      ,avg("Balance").as("Balance_avg")
      ,avg("ShotPower").as("ShotPower_avg")
      ,avg("Jumping").as("Jumping_avg")
      ,avg("Stamina").as("Stamina_avg")
      ,avg("Strength").as("Strength_avg")
      ,avg("LongShots").as("LongShots_avg")
      ,avg("Aggression").as("Aggression_avg")
      ,avg("Interceptions").as("Interceptions_avg")
      ,avg("Positioning").as("Positioning_avg")
      ,avg("Vision").as("Vision_avg")
      ,avg("Penalties").as("Penalties_avg")
      ,avg("Composure").as("Composure_avg")
      ,avg("Marking").as("Marking_avg")
      ,avg("StandingTackle").as("StandingTackle_avg")
      ,avg("SlidingTackle").as("SlidingTackle_avg")
      ,avg("GKDiving").as("GKDiving_avg")
      ,avg("GKHandling").as("GKHandling_avg")
      ,avg("GKKicking").as("GKKicking_avg")
      ,avg("GKPositioning").as("GKPositioning_avg")
      ,avg("GKReflexes").as("GKReflexes_avg")
    )

    val st_cols = df.select("Name","Overall","Position","Crossing","Finishing","HeadingAccuracy","ShortPassing","Volleys","Dribbling"
      ,"Curve","FKAccuracy","LongPassing","BallControl","Acceleration","SprintSpeed","Agility","Reactions","Balance","ShotPower"
      ,"Jumping","Stamina","Strength","LongShots","Aggression","Interceptions","Positioning","Vision","Penalties","Composure"
      ,"Marking","StandingTackle","SlidingTackle","GKDiving","GKHandling","GKKicking","GKPositioning","GKReflexes")
      .filter(col("Position")==="ST")

    val st_atts= st_cols.agg(avg("Crossing").as("Crossing_avg")
      ,avg("Finishing").as("Finishing_avg")
      ,avg("HeadingAccuracy").as("HeadingAccuracy_avg")
      ,avg("ShortPassing").as("ShortPassing_avg")
      ,avg("Volleys").as("Volleys_avg")
      ,avg("Dribbling").as("Dribbling_avg")
      ,avg("Curve").as("Curve_avg")
      ,avg("FKAccuracy").as("FKAccuracy_avg")
      ,avg("LongPassing").as("LongPassing_avg")
      ,avg("BallControl").as("BallControl_avg")
      ,avg("Acceleration").as("Acceleration_avg")
      ,avg("SprintSpeed").as("SprintSpeed_avg")
      ,avg("Agility").as("Agility_avg")
      ,avg("Reactions").as("Reactions_avg")
      ,avg("Balance").as("Balance_avg")
      ,avg("ShotPower").as("ShotPower_avg")
      ,avg("Jumping").as("Jumping_avg")
      ,avg("Stamina").as("Stamina_avg")
      ,avg("Strength").as("Strength_avg")
      ,avg("LongShots").as("LongShots_avg")
      ,avg("Aggression").as("Aggression_avg")
      ,avg("Interceptions").as("Interceptions_avg")
      ,avg("Positioning").as("Positioning_avg")
      ,avg("Vision").as("Vision_avg")
      ,avg("Penalties").as("Penalties_avg")
      ,avg("Composure").as("Composure_avg")
      ,avg("Marking").as("Marking_avg")
      ,avg("StandingTackle").as("StandingTackle_avg")
      ,avg("SlidingTackle").as("SlidingTackle_avg")
      ,avg("GKDiving").as("GKDiving_avg")
      ,avg("GKHandling").as("GKHandling_avg")
      ,avg("GKKicking").as("GKKicking_avg")
      ,avg("GKPositioning").as("GKPositioning_avg")
      ,avg("GKReflexes").as("GKReflexes_avg")
    )

    println("Below are the most number of left footed midfielders under 30 years of age")
    age_below_30_count.show()
    println("Most expensive squad in the world is "+exp_squad_final.head() +"and it has largest wage")
    println("The strongest team by overall rating for a 4-4-2 formation is :"+best_team.head())
    println("position pays the highest wage in average to "+pos_pay.head())
    println("4 attributes which are most relevant to becoming a good goalkeeper :\n"+Validate.getAttributes(avg_atts,4))
    println("\n5 attributes which are most relevant to becoming a top striker : \n"+Validate.getAttributes(st_atts,5))

    val props = new Properties()
    props.put("user", "postgres")
    props.put("password", "*****")
    props.put("driver", "org.postgresql.Driver")

    //Writing to postgres
    val postgres_data=df.withColumnRenamed("Overall","Overall Rating")
      .withColumnRenamed("Club","Club Name")
      .withColumnRenamed("Nationality","Country")
      .withColumn("Value",
          when(col("Value").endsWith("M"),regexp_extract(col("Value"),"\\d+",0)*1000000)
        .when(col("Value").endsWith("K"),regexp_extract(col("Value"),"\\d+",0)*1000))
      .withColumn("Wage",when(col("Wage").endsWith("K"),regexp_extract(col("Wage"),"\\d+",0)*1000))
      .select("Name", "Club Name", "Wage", "Value","Overall Rating","Joined", "Age","Country")

    postgres_data.write
      .mode(SaveMode.Overwrite)
      .jdbc(url = "jdbc:postgresql://hostname:port/dbname", table = "test", connectionProperties = props)

    df.unpersist()
  }


}
