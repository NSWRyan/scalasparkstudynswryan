package ryan.widodo

import com.github.tototoshi.csv.CSVWriter
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
class MainAppTest extends AnyFunSuite with BeforeAndAfter {
  // Create a temporary directory for testing
  var tempDir: String = _
  var tempTeleportFile: String = _
  var tempPassengerFile: String = _

  before {
    tempDir = Files.createTempDirectory("test-main-app").toString
    tempTeleportFile = s"$tempDir/teleportData.csv"
    tempPassengerFile = s"$tempDir/passengers.csv"

    // Sample data for Teleports and passengers
    val teleportsData: Seq[Seq[String]] = Seq(
      Seq("passengerId", "teleportId", "from", "to", "date"),
      Seq("1", "101", "zz", "fr", "2024-05-01"),
      Seq("2", "102", "us", "jp", "2024-06-01")
    )

    val passengersData: Seq[Seq[String]] = Seq(
      Seq("passengerId", "firstName", "lastName"),
      Seq("1", "John", "Doe"),
      Seq("2", "Jane", "Smith")
    )

    // Write sample CSV data
    val teleportWriter: CSVWriter = CSVWriter.open(tempTeleportFile)
    teleportWriter.writeAll(teleportsData)
    teleportWriter.close()

    val passengerWriter: CSVWriter = CSVWriter.open(tempPassengerFile)
    passengerWriter.writeAll(passengersData)
    passengerWriter.close()
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test(
    "main should exit with usage message if insufficient arguments are provided"
  ) {
    val originalSecurityManager: SecurityManager = System.getSecurityManager
    val originalConsoleOut: PrintStream = Console.out

    System.setSecurityManager(new NoExitSecurityManager)

    val outContent: ByteArrayOutputStream = new ByteArrayOutputStream()

    try {
      Console.withOut(new PrintStream(outContent)) {
        intercept[ExitException] {
          // Call MainApp with zero args.
          val args: Array[String] = Array[String]()
          MainApp.main(args)
        } match {
          case e: ExitException => assert(e.getStatus == 1)
        }
      }

      val capturedOutput: String = outContent.toString()
      assert(capturedOutput.contains("Usage"))
    } finally {
      System.setSecurityManager(originalSecurityManager)
    }
  }

  test("main function should create output files") {
    // Set up the output directory
    val outputDir = s"$tempDir/output"

    val args = Array[String](
      tempTeleportFile,
      tempPassengerFile,
      outputDir
    )
    // Call the main function
    MainApp.main(args)

    // Verify output files
    assert(Files.exists(Paths.get(outputDir, "question1.csv")))
    assert(Files.exists(Paths.get(outputDir, "question2Outer.csv")))
    assert(Files.exists(Paths.get(outputDir, "question2Inner.csv")))
    assert(Files.exists(Paths.get(outputDir, "question3.csv")))
    assert(Files.exists(Paths.get(outputDir, "question4.csv")))
    assert(Files.exists(Paths.get(outputDir, "question5.csv")))
  }
}
