package operations

import com.datastax.driver.core.{ConsistencyLevel, Row, Session}
import providers.CassandraConnection

import scala.collection.JavaConverters._

object Operations {

  val session: Session = CassandraConnection.cassandraSession
  session.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)

  /**
    * inserts record to Employee and cityEmployees table.
    *
    * @param id     id of employee.
    * @param name   name of employee.
    * @param city   city of employee.
    * @param salary salary of employee.
    * @param phone  phone of employee.
    * @return string of successful insertion.
    */
  def insertRecord(id: Int, name: String, city: String, salary: Int, phone: Long): String = {

    val insertCqlForEmployees = s"INSERT INTO Employees(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES($id,'$name','$city',$salary,$phone) IF NOT EXISTS "
    val insertCqlForCityEmployees = s"INSERT INTO CityEmployees(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES($id,'$name','$city',$salary,$phone) IF NOT EXISTS "
    session.execute(insertCqlForEmployees)
    session.execute(insertCqlForCityEmployees)
    s"inserted record for employee id $id"

  }

  /**
    * get employees for particular id.
    *
    * @param id id of employee.
    * @return employees.
    */

  def getEmployeeWithId(id: Int): List[Row] = {
    val selectCql = s"SELECT * FROM EMPLOYEES WHERE emp_id = $id"
    session.execute(selectCql).asScala.toList
  }

  /**
    * update city for employee.
    *
    * @param id     id for employee to update.
    * @param salary salary for employee to update
    * @param city   city that has to be set.
    * @return employees.
    */

  def updateEmployeeCity(id: Int, salary: Int, city: String): List[Row] = {
    val updateCqlForEmployees = s"UPDATE Employees SET emp_city = $city WHERE emp_id = $id and emp_salary = $salary"
    session.execute(updateCqlForEmployees).asScala.toList
  }

  /**
    * gets employee for given id and greater than given salary.
    *
    * @param id     id of employee to get.
    * @param salary salary greater to which get employee.
    * @return employees.
    */

  def getEmployeeWithIdAndGreaterSalary(id: Int, salary: Int): List[Row] = {
    val selectCqlForEmployees = s"SELECT * FROM EMPLOYEES WHERE emp_id = $id and emp_salary > $salary"
    session.execute(selectCqlForEmployees).asScala.toList
  }

  /**
    * get employee for given city.
    *
    * @param city city for which to get employees.
    * @return employees.
    */

  def getEmployeesWithCity(city: String): List[Row] = {
    val selectCql = s"SELECT * FROM EMPLOYEES WHERE emp_city = $city"
    session.execute(selectCql).asScala.toList
  }

  /**
    * delete employees for given city.
    *
    * @param city city for which to delete.
    * @return employees
    */
  def deleteFromCityEmployees(city: String): List[Row] = {
    val deleteCql = s"DELETE FROM CityEmployees WHERE emp_city = $city"
    session.execute(deleteCql).asScala.toList
  }


}
