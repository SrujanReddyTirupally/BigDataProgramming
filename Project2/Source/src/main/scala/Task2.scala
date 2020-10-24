import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Task2 {

  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task2")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Task2")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("Srujan").setLevel(Level.ERROR)

    // We are using all 3 Fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema

    val df_worldcups = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("WorldCups.csv")

    val df_players = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("WorldCupPlayers.csv")

    val df_matches = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("WorldCupMatches.csv")

    // Printing the Schema
    df_worldcups.printSchema()
    df_players.printSchema()
    df_matches.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames

    //First of all create three Temp View
    df_worldcups.createOrReplaceTempView("WC")
    df_players.createOrReplaceTempView("Players")
    df_matches.createOrReplaceTempView("Matches")


    // Find the winner by years using WorldCup view
    val Query = spark.sql("select Winner, Runners, Country, Year from WC Order By Country ")
    print("Query")
    Query.show()

    //Find the goals by years using WorldCup view
    val Query1 = spark.sql("select Winner,QualifiedTeams, MatchesPlayed, Year from WC WHERE Country = 'Germany' Order By Year")
    print("Query1")
    Query1.show()

    //teams that played most number of matches as home teams wcMatches
    val Query2 = spark.sql("select Count(HomeTeamName),HomeTeamName from Matches Group By HomeTeamName")
    print("Query2")
    Query2.show()

    //Teams with the most world cup final victories on WorldCup view
    val Query3 = spark.sql("select Count(Winner),Winner,Attendance from WC Group By Winner, Attendance")
    print("Query3")
    Query3.show()

    // Display all the participants in the year 2014
    val Query4 = spark.sql("select * from Matches where Stage='First round' AND Year  = 2014 ")
    print("Query4")
    Query4.show()

    //matches held by coach MILLAR Bob (USA)
    val Query5 =spark.sql("select * from Players where `Coach Name` = 'MILLAR Bob (USA)'")
    print("Query5")
    Query5.show()

    //No of matches in year 2014 and in Estadio do Maracana
    val Query6 = spark.sql("select count(*) from Matches where year=2014 AND Stadium = 'Estadio do Maracana' ")
    print("Query6")
    Query6.show()

    //number of matches that held in Estadio do Maracana stadium
    val Query7 = spark.sql("select count(*) from Matches where Stadium = 'Estadio do Maracana'")
    print("Query7")
    Query7.show()

    //Country which hoster World Cup highest number of times
    val Query8 = spark.sql("select Count(Country),Country,Year from WC Group by Country,Year")
    print("Query8")
    Query8.show()

    //Stadium with highest number of matches
    val Query9 = spark.sql("select Count(Stadium),Stadium from Matches Group By Stadium")
    print("Query9")
    Query9.show()

    val Query10 = spark.sql("select `Player Name`, Position from Players where Position = 'C' ")
    print("Query10")
    Query10.show()



    //HomeTeam Goals Count and their stage by Years
    val Query11 = spark.sql("select `HomeTeamName`,Stage,Year FROM Matches Group By Year,`HomeTeamName`,Stage")
    print("Query11")
    Query11.show()

    // Away Team Goals and their stage
    val Query12 = spark.sql("select `Away Team Name`,Stage,Year from Matches Group By Year,`Away Team Name`,Stage")
    print("Query12")
    Query12.show()


    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.
    // To Solve this Problem we first create the rdd as we already have Dataframe df1 created above code
    // RDD creation

    val file = sc.textFile("WorldCups.csv")

    val h1 = file.first()

    val data = file.filter(line => line != h1)
    println("RDD")
    data.foreach(println)



    val rdd = data.map(line=>line.split(",")).collect()

    println("Query1")
    //RDD Highest Numbers of goals
    val rdd1 = data.filter(line => line.split(",")(0) == "2014").map(line => (line.split(",")(0),
      (line.split(",")(1)), (line.split(",")(2)), (line.split(",")(3)) ) )
    println("Using rdd")
    rdd1.foreach(println)

    // Dataframe
    println("Using Data frame")
    df_worldcups.select("Year","Country", "Winner").filter("Year =2014").show(10)

    // Dataframe SQL
    val DFQ1 = spark.sql("select Year, Country, Winner FROM WC WHERE Year = 2014 order by Year Desc Limit 10")
    println("Using SQL on Data frame")
    DFQ1.show()

    // Year, Venue country = winning country
    // Using RDD
    println("Query2")
    val rdd2 = data.filter(line => (line.split(",")(2)=="Germany" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3),line.split(",")(4),line.split(",")(5)
        ,line.split(",")(5))).collect()
    println("Using rdd")
    rdd2.foreach(println)

    // Using Dataframe
    println("Using Data frame")
    df_worldcups.select("Year","Winner","Runners","Third", "Fourth").filter("Winner == 'Germany'").show(10)

    // usig Spark SQL
    val DFQ2 = spark.sql("select * from WC where Winner = 'Germany' order by Year")
    println("Using SQL on Data frame")
    DFQ2.show(10)

    // Details of years ending in ZERO
    // RDD
    val rdd3 = data.filter(line => (line.split(",")(7)>"16" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(6), line.split(",")(7))).collect()
    println("Using rdd")
    rdd3.foreach(println)

    //DataFrame
    println("Using Data frame")
    df_worldcups.select("Year","Winner","QualifiedTeams").filter("QualifiedTeams > 16")
      df_worldcups.show(10)
    //DF - SQL
    val DFQ3 = spark.sql("SELECT Year, Winner, QualifiedTeams from WC where QualifiedTeams > 16  ")
    println("Using SQL on Data frame")
    DFQ3.show(10)
    //2014 world cup stats
    //Rdd
    val rdd4 = data.filter(line => line.split(",")(1)==line.split(",")(5))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(5)))
      .collect()
    println("Using rdd")
    rdd4.foreach(println)

    // Using Dataframe
    println("Using Data frame")
    df_worldcups.select("Year","Country","Fourth").filter("Country==Fourth").show(10)

    // usig Spark SQL
    val DFQ4 = spark.sql("select Year,Country,Fourth from WC where Country = Fourth order by Year")
    println("Using SQL on Data frame")
    DFQ4.show()

    //Max matches played
    //RDD
    val rdd5 = data.filter(line=>line.split(",")(8) > "55")
      .map(line=> (line.split(",")(0),line.split(",")(8),line.split(",")(3))).collect()
    println("Using rdd")
    rdd5.foreach(println)

    // DataFrame
    println("Using Data frame")
    df_worldcups.filter("MatchesPlayed > 55").show()

    // Spark SQL
    val DFQ5 = spark.sql(" Select * from WC where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WC )" )
    println("Using SQL on Data frame")
    DFQ5.show()

  }

}