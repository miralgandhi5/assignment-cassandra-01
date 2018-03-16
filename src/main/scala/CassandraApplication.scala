import operations.Operations

object CassandraApplication {
  def main(args: Array[String]): Unit = {

    val idOne = 1
    val nameOne = "miral"
    val cityOne = "delhi"
    val salaryOne = 30000
    val phoneOne = 88990099
    val idTwo = 2
    val nameTwo = "rahul"
    val cityTwo = "chandigarh"
    val salaryTwo = 40000
    val phoneTwo = 88990099
    print(s"${Operations.insertRecord(idOne,nameOne,cityOne,salaryOne,phoneOne)}")
    print(s"${Operations.insertRecord(idTwo,nameTwo,cityTwo,salaryTwo,phoneTwo)}")
    print(s"${Operations.getEmployeeWithId(idOne)}")
    print(s"${Operations.updateEmployeeCity(idOne,salaryOne,cityTwo)}")
    print(s"${Operations.getEmployeeWithIdAndGreaterSalary(idOne,salaryOne)}")
    print(s"${Operations.getEmployeesWithCity(cityTwo)}")
    print(s"${Operations.deleteFromCityEmployees(cityTwo)}")




  }

}
