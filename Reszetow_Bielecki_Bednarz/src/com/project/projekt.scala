package com.project

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object projekt {
  
  case class Daily(
      wban_Number:String, 
      YearMonthDay:String,
      Max_Temp:String,
      Min_Temp:String, 
      Avg_Temp:Double, 
      Dep_from_Normal:String,
      Avg_Dew_Pt:String, 
      Avg_Wet_Bulb:String, 
      Heating_Degree_Days:String, 
      Cooling_Degree_Days:String, 
      Significant_Weather:String,
      Snow_Ice_Depth:String, 
      Snow_Ice_Water_Equiv:String, 
      Precipitation_Snowfall:String, 
      Precipitation_Water_Equiv:String, 
      Pressue_Avg_Station:String,
      Pressure_Avg_Sea_Level:String,
      Wind_Speed:String,
      Wind_Direction:String, 
      Wind_Avg_Speed:Double, 
      Max_5_sec_speed:String,
      Max_5_sec_Dir:String,
      Max_2_min_speed:String,
      Max_2_min_Dir:String)
      
  case class dailyAvg(
      WbanNumber:String,
      YearMonthDay:String,
      AvgMaxTemp:String,
      DepartureMaxTemp:String,
      AvgMinTemp:String,
      DepartureMinTemp:String,
      AvgTemp:Double,
      DeparturefromNormal:String,
      AvgDewPoint:String,
      AvgWetBulb:String,
      HeatingDegreeDays:String,
      CoolingDegreeDays:String,
      HDDMonthlyDeparture:String,
      CDDMonthlyDeparture:String,
      HDDSeasontoDate:String,
      CDDSeasontoDate:String,
      HDDSeasontoDateDeparture:String,
      CDDSeasontoDateDeparture:String,
      MeanStationPressure:String,
      MeanSeaLevelPressure:String,
      MaxSeaLevelPressure:String,
      DateMaxSeaLevelPressure:String,
      TimeMaxSeaLevelPressure:String,
      MinSeaLevelPressure:String,
      DateMinSeaLevelPressure:String,
      TimeMinSeaLevelPressure:String,
      TotalMonthlyPrecip:String,
      DeparturefromNormalPrecip:String,
      Max24hrPrecip:String,
      DateMax24hrPrecip:String,
      SunshineMinutes:String,
      TotalMinutesSunshine:String,
      PercentPossibleSunshine:String,
      TotalSnowfall:String,
      Max24hrSnowfall:String,
      DateMax24hrSnowfall:String,
      Max12ZSnowDepth:String,
      DateMax12ZSnowDepth:String,
      ClearSkyDays:String,
      PartlyCloudyDays:String,
      CloudyDays:String,
      MaxTemp90Days:String,
      MaxTemp32Days:String,
      MinTemp32Days:String,
      MinTemp0Days:String,
      ThunderstormDays:String,
      HeavyFogDays:String,
      Precip1100Days:String,
      Precip110Days:String,
      Snowfall10inchDays:String,
      MaxWindSpeed:String,
      MaxWindDirection:String,
      MaxWindDate:String,
      PeakGustSpeed:String,
      PeakGustDirection:String,
      PeakGustDate:String,
      WaterEquivalent:String,
      ResultantWindSpeed:String,
      ResultantWindDirection:String,
      AvgWindSpeed:Double,
      AvgHDD:String,
      AvgCDD:String)
      
  
  case class Hour(
      WbanNumber:String, 
      YearMonthDay:String, 
      Time:String, 
      StationType:String, 
      MaintenanceIndicator:String, 
      SkyConditions:String, 
      Visibility:String, 
      WeatherType:String, 
      DryBulbTemp:Double, 
      DewPointTemp:Double, 
      WetBulbTemp:Double, 
      RelativeHumidity:String, 
      WindSpeed:Double, 
      WindDirection:String,
      WindCharGusts:String, 
      ValforWindChar:String, 
      StationPressure:String, 
      PressureTendency:String, 
      SeaLevelPressure:String,
      RecordType:String, 
      PrecipTotal:String)
      
      
      
  def mapper_Daily(line:String): Option[Daily] = {
    val fields = line.split(',')
    if(fields.length > 1 && fields(4)!="M" && fields(19)!="M" && fields(4)!="-" && fields(19)!="-"){
      return Some(Daily(fields(0), fields(1),fields(2), fields(3),
          fields(4).toDouble,fields(5),fields(6),
          fields(7),fields(8),fields(9),
          fields(10), fields(11), fields(12),fields(13),fields(14),
          fields(15),fields(16),fields(17),
          fields(18),fields(19).toDouble,fields(20),
          fields(21),fields(22),fields(23)))
    } else {
      return None
    }
  }
  
    def mapper_AVG(line:String): Option[dailyAvg] = {
    val fields = line.split(',')
    if(fields.length > 1 && fields(6)!="M" && fields(59)!="M" && fields(6)!="-" && fields(59)!="-" && !fields(6).isEmpty() && !fields(59).isEmpty()){
      try{
      val Return = Some(dailyAvg(
          fields(0), fields(1),fields(2), fields(3),
          fields(4),fields(5),fields(6).toDouble,
          fields(7),fields(8),fields(9),
          fields(10), fields(11), fields(12),fields(13),fields(14),
          fields(15),fields(16),fields(17),
          fields(18),fields(19),
          fields(20),fields(21),fields(22),fields(23),
          fields(24),fields(25),fields(26),
          fields(27),fields(28),fields(29),
          fields(30),fields(31),fields(32),fields(33),
          fields(34),fields(35),fields(36),
          fields(37),fields(38),fields(39),
          fields(40),fields(41),fields(42),fields(43),
          fields(44),fields(45),fields(46),
          fields(47),fields(48),fields(49),
          fields(50),fields(51),fields(52),fields(53),
          fields(54),fields(55),fields(56),
          fields(57),fields(58),fields(59).toDouble,
          fields(60),fields(61)))
      return Return}
      catch{
        case e:Exception=>{return None}        
      }
    } else {
      return None
    }
  }
    
   def mapper_Hour(line:String): Option[Hour] = {
    val fields = line.split(',')
    if(fields.length > 1 && fields(8)!="M" && fields(9)!="M" && fields(10)!="M" && fields(12)!="M" && 
        fields(8)!="-" && fields(9)!="-" && fields(10)!="-" && fields(12)!="-" &&
        !fields(8).contains("/0") && !fields(9).contains("/0") && !fields(10).contains("/0") && !fields(12).contains("/0")){
      return Some(Hour(fields(0), fields(1),fields(2), fields(3),
          fields(4),fields(5),fields(6),
          fields(7),fields(8).toDouble,fields(9).toDouble,
          fields(10).toDouble, fields(11), fields(12).toDouble,fields(13),fields(14),
          fields(15),fields(16),fields(17),
          fields(18),fields(19),fields(20)))
    } else {
      return None
    }
  } 
    
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("projektASE")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    //Daily
    val linesDaily = spark.sparkContext.textFile("./daily.txt")  
    val headerDaily = linesDaily.first()
    val linesDaily2 = linesDaily.filter(x => x != headerDaily)
    val dataDaily = linesDaily2.flatMap(mapper_Daily)
    
    //Hourly
    val linesHour = spark.sparkContext.textFile("./hourly.txt")  
    val headerHour = linesHour.first()
    val linesHour2 = linesHour.filter(x => x != headerHour)
    val dataHour = linesHour2.flatMap(mapper_Hour)
    
    //AVG
    val linesAVG = spark.sparkContext.textFile("./dailyAvg.txt")  
    val headerAVG = linesAVG.first()
    val linesAVG2 = linesAVG.filter(x => x != headerAVG)
    val dataAVG = linesAVG2.flatMap(mapper_AVG)
    
    //DFy
    import spark.implicits._
    val DFdailyData = dataDaily.toDF
    val DFhourData = dataHour.toDF
    val DFavgData = dataAVG.toDF
    val wyswietl = DFavgData.collect()
    println(wyswietl)
    
    
    println(" ")
    //Daily
    val countDaily = DFdailyData.count()
    val averageTemperatureSumDaily = DFdailyData.select(col("Avg_Temp")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val averageTempDaily = averageTemperatureSumDaily / countDaily
    print("Srednia temperatura plik Daily: ")
    println(averageTempDaily)    
    val averageWindSumDaily = DFdailyData.select(col("Wind_Avg_Speed")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val averageWindDaily = averageWindSumDaily / countDaily
    print("Srednia predkosc wiatru plik Daily: ")
    println(averageWindDaily)
    
    println(" ")
    //Hourly
    val countHour = DFhourData.count()
    val DryTemperatureSumHour = DFhourData.select(col("DryBulbTemp")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val DryBulbTempHour = DryTemperatureSumHour / countHour
    print("Srednia Dry temperatura plik Hour: ")
    println(DryBulbTempHour)   
    val DewTemperatureSumHour = DFhourData.select(col("DewPointTemp")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val DewBulbTempHour = DewTemperatureSumHour / countHour
    print("Srednia Dew temperatura plik Hour: ")
    println(DewBulbTempHour)   
    val WetTemperatureSumHour = DFhourData.select(col("WetBulbTemp")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val WetBulbTempHour = WetTemperatureSumHour / countHour
    print("Srednia Wet temperatura plik Hour: ")
    println(WetBulbTempHour)   
    val averageTempHour = (DryBulbTempHour+DewBulbTempHour+WetBulbTempHour)/3
    print("Srednia temperatura powyzszych plik Hour: ")
    println(averageTempHour)
    val averageWindSumHour = DFhourData.select(col("WindSpeed")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val averageWindHour = averageWindSumHour / countHour
    print("Srednia predkosc wiatru plik Hour: ")
    println(averageWindHour)
    
    println(" ")
    //AVG
    val countAVG = DFavgData.count()
    val averageTemperatureSumAVG = DFavgData.select(col("AvgTemp")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val averageTempAVG = averageTemperatureSumAVG / countAVG
    print("Srednia temperatura plik AVG: ")
    println(averageTempAVG)    
    val averageWindSumAVG = DFavgData.select(col("AvgWindSpeed")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val averageWindAVG = averageWindSumAVG / countAVG
    print("Srednia predkosc wiatru plik AVG: ")
    println(averageWindAVG)
    
   
     val plot_maker = new plot(averageTempAVG, averageWindAVG,averageTempDaily,
      averageTempHour, averageWindDaily, averageWindHour, DryBulbTempHour, DewBulbTempHour, WetBulbTempHour)
    
    
    plot_maker.main(args: Array[String])
    
    spark.stop()
    
 
  }
  
   
}