package day04.test

object MyPredef {

  @inline implicit val name: String = "李四"

  @inline implicit def person(message:String): Persons = new Persons(message)



}
