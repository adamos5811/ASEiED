import com.project.Kmeans
import org.scalatest.{FunSpec, GivenWhenThen}
import org.apache.log4j.Logger
import org.apache.log4j.Level


class KmeansTest extends FunSpec with GivenWhenThen {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  describe("KmeansTest") {
    val sj = new Kmeans
    sj.k_mean

  }

}
