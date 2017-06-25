import com.ts.SortingJob
import org.scalatest.{FunSpec, GivenWhenThen}



class SortJobTest extends FunSpec with GivenWhenThen {

  describe("SortTest") {
    val sj = new SortingJob
    sj.quickSortMaint
    sj.selectionSort
  }

}