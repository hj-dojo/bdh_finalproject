/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.util.Date

case class ICUStay(subjectID:Int, hadmID:Int, icustayID:Int, intime: Date, outtime: Date)

case class ChartEvents(subjectID:Int, hadmID:Int, icustayID:Int, charttime: Date, value: Double)
