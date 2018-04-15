/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.sql.Timestamp

case class ICUStay(subjectID:Int, hadmID:Int, icustayID:Int, inTime: Timestamp, outTime: Timestamp,
                   dod: Timestamp, age: Double, gender: String, expired: Int)

case class ChartEvents(subjectID:Int, hadmID:Int, icustayID:Int, itemID:Int, chartTime: Timestamp, value: Double)

case class Prescriptions(subjectID:Int, hadmID:Int, icustayID:Int, startDate: Timestamp, endDate: Timestamp,
                         drug: String, value: String, unit: String)

case class MicrobiologyEvents(subjectID:Int, hadmID:Int, chartDate: Timestamp)