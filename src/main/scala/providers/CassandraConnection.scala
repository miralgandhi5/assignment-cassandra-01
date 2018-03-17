package providers

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}

object CassandraConnection {

  val config: Config = ConfigFactory.load()
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraHostname: String = config.getString("cassandra.contact.points")

  val cassandraSession: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(cassandraHostname).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  $cassandraKeyspace WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE $cassandraKeyspace")

    //database modelling
    session.execute(s"CREATE TABLE IF NOT EXISTS Employees (emp_id int, emp_name text,emp_city text,emp_salary varint, emp_phone varint ,PRIMARY KEY(emp_id,emp_salary) )")
    session.execute(s"CREATE TABLE IF NOT EXISTS CityEmployees (emp_id int, emp_name text,emp_city text,emp_salary varint, emp_phone varint ,PRIMARY KEY(emp_city) )")
    session
  }

}
