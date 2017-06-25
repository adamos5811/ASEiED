import com.ts.SortingJob
import org.scalatest.{FunSpec, GivenWhenThen}
import org.apache.log4j.Logger
import org.apache.log4j.Level




class SortJobTest extends FunSpec with GivenWhenThen {
  // ograniczenie ilosci wyswietlanych logow
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  describe("SortTest") {
    val sj = new SortingJob
    sj.quickSortMaint
    sj.selectionSort
    sj.sparkSort
  }

}