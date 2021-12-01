import scala.util.Random
import java.time._

object DateTimeGenerator {
  def generate (): String = {
        val r1 = new Random()
        val days = r1.nextInt(366)
        val minutes = r1.nextInt(1440)
        val dateStart = LocalDate.of(2020, 11, 30)
        val timeStart = LocalTime.of(0, 0, 0)
        val Date = dateStart.plusDays(days)
        val Time = timeStart.plusMinutes(minutes)
        Date + " " + Time
    }
}
