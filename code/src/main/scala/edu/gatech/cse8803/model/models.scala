/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.sql.Date

case class ICUStay(subjectID:Int, hadmID:Int, icustayID:Int, inTime: Date, outTime: Date)

case class ChartEvents(subjectID:Int, hadmID:Int, icustayID:Int, itemID:Int, chartTime: Date, value: Double)

case class Prescriptions(subjectID:Int, hadmID:Int, icustayID:Int, startDate: Date, endDate: Date,
                         drug: String, value: String, unit: String)

case class MicrobiologyEvents(subjectID:Int, hadmID:Int, chartDate: Date)

case class SuspicionOfInfection(subjectID:Int, hadmID:Int, icustayID:Int, indexDate: Date)