package com.project



import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.event.ActionEvent
import scalafx.scene.Scene
import scalafx.scene.control.Button
import scalafx.scene.shape.Rectangle
import scalafx.scene.paint.Color
import scalafx.scene.control.Label

class plot(averageTempAVG: Double, averageWindAVG: Double, averageTempDaily: Double,
      averageTempHour: Double, averageWindDaily: Double, averageWindHour: Double,
      DryBulbTempHour: Double, DewBulbTempHour: Double, WetBulbTempHour: Double) extends JFXApp {
  
  
 val ATH = BigDecimal(averageTempHour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val ATD =  BigDecimal(averageTempDaily).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val ATM =  BigDecimal(averageTempAVG).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 
 val AWH =  BigDecimal(averageWindHour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val AWD =  BigDecimal(averageWindDaily).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val AWM =  BigDecimal(averageWindAVG).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

 val DryT=   BigDecimal(DryBulbTempHour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val DewT = BigDecimal(DewBulbTempHour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
 val WetT = BigDecimal(WetBulbTempHour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  
  
 var MDHchart_startT = 50
 var MDHchart_startW = 350
 var MDHchart_width = 70
 var MDHgap = 10
 
 var DDWchart_start = 100
 var DDWchart_width = 120
 var DDWgap = 30
 
 
 
 
 // bule
 var DailyChartsVisibility = false
  var MonthlyChartsVisibility = false
    var HourlyChartsVisibility = false
 
  
  stage = new JFXApp.PrimaryStage {
    title.value = "Weather Analysis"
    width = 650
    height = 650
    scene = new Scene {
     
     // menu buttons
      
       var MDH_button = new Button{
       text="Month - Daily - Hourly"
       layoutX = 200
       layoutY = 200
       }
       
       var DDW_button = new Button{
       text="Dry - Dew - Wet"
       layoutX = 200
       layoutY = 250
       }
       
       var AI_button = new Button{
       text="Additional Info"
       layoutX = 200
       layoutY = 300
       }
       
       
      // butttony MDH
       
       var AverageMonthly_button = new Button{
       text="Monthly Average"
       layoutX = 100
       layoutY = 50
       }
       
       var AverageDaily_button = new Button{
       text="Daily Average"
       layoutX = 250
       layoutY = 50
       }
       
       var AverageHourly_button = new Button{
       text="Hourly Average"
       layoutX = 400
       layoutY = 50
       }
       
       // get back button
       
        var GetBack_button = new Button{
       text="Get Back"
       layoutX = 260
       layoutY = 100
       }
       
        // recty MDH
        
        var MonthlyTemp_rect = new Rectangle {
          
          x = MDHchart_startT
          y = 500 - ATM*4
          width = MDHchart_width
          height = ATM*4
          fill = Color.Gray.darker
          
        }
        
        var DailyTemp_rect = new Rectangle {
          
          x = MDHchart_startT + ( MDHgap + MDHchart_width) 
          y = 500 - ATD*4
          width = MDHchart_width
          height = ATD*4
          fill = Color.Gray
          
        }
        
          var HourlyTemp_rect = new Rectangle {
          
          x = MDHchart_startT + 2*(MDHgap + MDHchart_width) 
          y = 500 - ATH*4
          width = MDHchart_width
          height = ATH*4
          fill = Color.LightGray
          
        }
        
          //  windy MDH
          
         var MonthlyWind_rect = new Rectangle {
          
          x = MDHchart_startW
          y = 500 - AWM*4
          width = MDHchart_width
          height = AWM*4
          fill = Color.Blue.darker
          
        }
        
        var DailyWind_rect = new Rectangle {
          
          x = MDHchart_startW + ( MDHgap + MDHchart_width) 
          y = 500 - AWD*4
          width = MDHchart_width
          height = AWD*4
          fill = Color.Blue
          
        }
        
          var HourlyWind_rect = new Rectangle {
          
          x = MDHchart_startW + 2*(MDHgap + MDHchart_width) 
          y = 500 - AWH*4
          width = MDHchart_width
          height = AWH*4
          fill = Color.LightBlue
          
        }
          // DDW rects
          
          var DryTemp_rect = new Rectangle {
          
          x = DDWchart_start 
          y = 500 - DryT*4
          width = DDWchart_width
          height = DryT*4
          fill = Color.Orange
          
        }
          
         var DewTemp_rect = new Rectangle {
          
          x = DDWchart_start + (DDWgap + DDWchart_width) 
          y = 500 - DewT*4
          width = DDWchart_width
          height = DewT*4
          fill = Color.LightBlue
          
        }
         
         var WetTemp_rect = new Rectangle {
          
          x = DDWchart_start + 2*(DDWgap + DDWchart_width) 
          y = 500 - WetT*4
          width = DDWchart_width
          height = WetT*4
          fill = Color.Blue
          
        }
          
        
        // labele
         
          var DryTemp_label = new Label{
          
          text = DryT.toString()
          layoutX = DDWchart_start +50
          layoutY = DryTemp_rect.y()-20
          
        }
             var DewTemp_label = new Label{
          
          text = DewT.toString()
          layoutX = DDWchart_start + (DDWgap + DDWchart_width) +50
          layoutY = DewTemp_rect.y()-20
          
        }
                var WetTemp_label = new Label{
          
          text = WetT.toString()
          layoutX = DDWchart_start+ 2*(DDWgap + DDWchart_width)  +50
          layoutY = WetTemp_rect.y()-20
          
        }
           var Dry_label = new Label{
          
          text = "DRY"
          layoutX = DDWchart_start + 50
          layoutY = DryTemp_rect.y() + DryTemp_rect.height() + 10
          
        }
             var Dew_label = new Label{
          
          text = "Dew"
          layoutX = DDWchart_start+ (DDWgap + DDWchart_width)  +50
          layoutY = DewTemp_rect.y() + DewTemp_rect.height() + 10
          
        }
                
          var Wet_label = new Label{
          
          text = "WET"
          layoutX = DDWchart_start+ 2*(DDWgap + DDWchart_width)  +50
          layoutY = WetTemp_rect.y() + WetTemp_rect.height() + 10
          
        }
        
         
       // labele MDH
          
        var MDHTemp_label = new Label{
          
          text = "TEMPERATURE"
          layoutX = MDHchart_startT
          layoutY = 560
          
        }
        
         var MDHWind_label = new Label{
          
          text = "WIND"
          layoutX = MDHchart_startW
          layoutY = 560
          
        }
          
          
         
          
          var MonthlyTemp_label = new Label{
          
          text = ATM.toString()
          layoutX = MDHchart_startT +23
          layoutY = MonthlyTemp_rect.y()-20
          
        }
          
            var DailyTemp_label = new Label{
          
          text = ATD.toString()
          layoutX = MDHchart_startT + (MDHgap + MDHchart_width) + 23
          layoutY = DailyTemp_rect.y()-20
          
        }
            
          var HourlyTemp_label = new Label{
          
          text = ATH.toString()
          layoutX = MDHchart_startT + 2*(MDHgap + MDHchart_width) + 23
          layoutY = HourlyTemp_rect.y()-20
          
        }
          
          var MonthlyWind_label = new Label{
          
          text = AWM.toString()
          layoutX = MDHchart_startW +23
          layoutY = MonthlyWind_rect.y()-20
          
        }
          
            var DailyWind_label = new Label{
          
          text = AWD.toString()
          layoutX = MDHchart_startW + (MDHgap + MDHchart_width) + 23
          layoutY = DailyWind_rect.y()-20
          
        }
            
          var HourlyWind_label = new Label{
          
          text = AWH.toString()
          layoutX = MDHchart_startW + 2*(MDHgap + MDHchart_width) + 23
          layoutY = HourlyWind_rect.y()-20
          
        }
          
        //additional info labels
            var TEMP_label = new Label{
          
          text = "TEMEPERATURE"
          layoutX = 100
          layoutY = 200
          scaleX = 1.2
          scaleY = 1.2
          
        }
            
          var WIND_label = new Label{
          
          text = "WIND"
          layoutX = 100
          layoutY = 400
          scaleX = 1.2
          scaleY = 1.2
        }
           var D_to_M_deviationT_label = new Label{
          
          text = "Daily to Monthly deviation: " + (ATD - ATM).toString()
          layoutX = 100
          layoutY = 250
        }
          var H_to_M_deviationT_label = new Label{
          
          text = "Hourly to Monthly deviation: " + (ATH - ATM).toString()
          layoutX = 100
          layoutY = 300
        }
          
          var H_to_D_deviationT_label = new Label{
          
          text = "Hourly to Daily deviation: " + (ATH - ATM).toString()
          layoutX = 100
          layoutY = 350
        }
          
         var D_to_M_deviationW_label = new Label{
          
          text = "Daily to Monthly deviation: " + (AWD - AWM).toString()
          layoutX = 100
          layoutY = 450
        }
          var H_to_M_deviationW_label = new Label{
          
          text = "Hourly to Monthly deviation: " + (AWH - AWM).toString()
          layoutX = 100
          layoutY = 500
        }
          
          var H_to_D_deviationW_label = new Label{
          
          text = "Hourly to Daily deviation: " + (AWH - AWD).toString()
          layoutX = 100
          layoutY = 550
        }
         
         
         
        
        // akcje buttonow
        
        GetBack_button.onAction = (e:ActionEvent) => {
          content = List(MDH_button, DDW_button, AI_button)
        }
        
        MDH_button.onAction = (e:ActionEvent) => {
          content = List(MDHTemp_label, MDHWind_label, AverageDaily_button, AverageHourly_button, AverageMonthly_button, GetBack_button)
        }
        AI_button.onAction = (e:ActionEvent) => {
          content = List( GetBack_button, H_to_D_deviationT_label, H_to_D_deviationW_label, H_to_M_deviationT_label,
              H_to_M_deviationW_label, D_to_M_deviationT_label, D_to_M_deviationW_label, TEMP_label, WIND_label)
        }
        DDW_button.onAction = (e:ActionEvent) => {
          content = List(GetBack_button, Dry_label, Dew_label, Wet_label, DryTemp_label, DryTemp_rect, DewTemp_label, DewTemp_rect, WetTemp_label, WetTemp_rect)
        }
       
        AverageMonthly_button.onAction = (e:ActionEvent) => {
         if(!MonthlyChartsVisibility)
         {
           content ++= List(MonthlyTemp_rect, MonthlyWind_rect, MonthlyTemp_label, MonthlyWind_label )
           MonthlyChartsVisibility = true
         }
         else{
           content --= List(MonthlyTemp_rect, MonthlyWind_rect, MonthlyTemp_label, MonthlyWind_label)
           MonthlyChartsVisibility = false
         }
        }
        
         AverageDaily_button.onAction = (e:ActionEvent) => {
         if(!DailyChartsVisibility)
         {
           content ++= List(DailyTemp_rect, DailyWind_rect, DailyTemp_label, DailyWind_label )
           DailyChartsVisibility = true
         }
         else{
           content --= List(DailyTemp_rect, DailyWind_rect, DailyTemp_label, DailyWind_label)
           DailyChartsVisibility = false
         }
        }
         
         AverageHourly_button.onAction = (e:ActionEvent) => {
         if(!HourlyChartsVisibility)
         {
           content ++= List(HourlyTemp_rect, HourlyWind_rect, HourlyTemp_label, HourlyWind_label )
           HourlyChartsVisibility = true
         }
         else{
           content --= List(HourlyTemp_rect, HourlyWind_rect, HourlyTemp_label, HourlyWind_label )
           HourlyChartsVisibility = false
         }
        }
        
        
        
        
        
      content = List(MDH_button, DDW_button, AI_button)
      
      }
      
      
  }
 
}