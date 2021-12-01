import scala.util.Random
import java.time._

object DateTimeGenerator {
  def generate (): String = {
    val r1 = new Random()
    val x = r1.nextInt(5)
    var days = 0
    if (x == 1) {
      days = r1.nextInt(30)
    }
    else {
      days = r1.nextInt(366)
    }
    val minutes = r1.nextInt(1440)
    val dateStart = LocalDate.of(2020, 11, 25)
    val timeStart = LocalTime.of(0, 0, 0)
    val Date = dateStart.plusDays(days)
    val Time = timeStart.plusMinutes(minutes)
    Date + " " + Time
  }

}
