package edu.knoldus

import java.util.{Calendar, Date}

object TimeConversion {

  def year (time: Long): Int = {
    val date = new Date (time * 1000L)
    val calendarObject = Calendar.getInstance
    calendarObject.setTime (date)
    calendarObject.get (Calendar.YEAR)
  }

  def month (time: Long): Int = {
    val date = new Date (time * 1000L)
    val calendarObject = Calendar.getInstance
    calendarObject.setTime (date)
    calendarObject.get (Calendar.MONTH) + 1
  }

  def day (time: Long): Int = {
    val date = new Date (time * 1000L)
    val calendarObject = Calendar.getInstance
    calendarObject.setTime (date)
    calendarObject.get (Calendar.DAY_OF_MONTH)
  }
}
