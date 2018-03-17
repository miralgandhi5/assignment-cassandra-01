import operations.Operations
import org.apache.log4j.Logger

object CassandraApplication {

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(this.getClass)
    val idOne = 1
    val nameOne = "miral"
    val cityOne = "delhi"
    val salaryOne = 50000
    val phoneOne = 88990099
    val idTwo = 2
    val nameTwo = "rahul"
    val cityTwo = "chandigarh"
    val salaryTwo = 40000
    val phoneTwo = 88990099
    val idThree = 3
    val nameThree = "ayush"
    val cityThree = "agra"
    val salaryThree = 50000
    val phoneThree = 77990099
    val idFour = 4
    val nameFour = "manjot"
    val cityFour = "delhi"
    val salaryFour = 44000
    val phoneFour = 98990099
    log.info(s"${Operations.insertRecord(idOne, nameOne, cityOne, salaryOne, phoneOne)}\n")
    log.info(s"${Operations.insertRecord(idTwo, nameTwo, cityTwo, salaryTwo, phoneTwo)}\n")
    log.info(s"${Operations.insertRecord(idThree, nameThree, cityThree, salaryThree, phoneThree)}\n")
    log.info(s"${Operations.insertRecord(idFour, nameFour, cityFour, salaryFour, phoneFour)}\n\n")
    log.info(s"get employee with id ${Operations.getEmployeeWithId(idOne)}\n\n")
    log.info(s"update employees${Operations.updateEmployeeCity(idOne, salaryOne, cityTwo)}\n\n")
    log.info(s"get employee with id $idOne and salary greater than $salaryTwo ${Operations.getEmployeeWithIdAndGreaterSalary(idOne, salaryTwo)}\n\n")
    log.info(s"get employees with city $cityTwo ${Operations.getEmployeesWithCity(cityTwo)}\n\n")
    log.info(s"delete cityEmployees wth city $cityTwo ${Operations.deleteFromCityEmployees(cityTwo)}")
    Operations.session.close()
  }

}
