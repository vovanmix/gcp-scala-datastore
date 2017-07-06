package io.applicative.datastore.util.reflection

import com.google.cloud.datastore.{Key => CloudKey}
import io.applicative.datastore.{BaseEntity, Key}
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import scala.collection.immutable.Seq


class ReflectionHelperSpec extends Specification with Mockito {
  private val helper = new ReflectionHelper {}
  private val testInstance = TestClass()

  "ReflectionHelper" should {
    "extract correct class from type parameter" in {
      val clazz = helper.extractRuntimeClass[TestClass]()
      clazz shouldEqual classOf[TestClass]
    }

    "convert objects with supported fields to com.google.cloud.datastore.Entity" in {
      val key = Key(CloudKey.newBuilder("test", "TestClass", "test").build())
      val entity = helper.instanceToDatastoreEntity(key, testInstance, classOf[TestClass])
      entity.hasKey shouldEqual true
      entity.getNames.size() shouldEqual 13
      entity.getLong("byteVal") shouldEqual testInstance.byteVal
      entity.getLong("intVal") shouldEqual testInstance.intVal
      entity.getDouble("doubleVal") shouldEqual testInstance.doubleVal
      entity.getDouble("floatVal") shouldEqual testInstance.floatVal
      entity.getString("stringVal") shouldEqual testInstance.stringVal
      entity.getDateTime("googleDateTimeVal") shouldEqual testInstance.googleDateTimeVal
      entity.getBoolean("boolVal") shouldEqual testInstance.boolVal
    }

    "convert com.google.cloud.datastore.Entity to object type E" in {
      val key = Key(CloudKey.newBuilder("test", "TestClass", "test").setId(testInstance.id).build())
      val clazz = classOf[TestClass]
      val entity = helper.instanceToDatastoreEntity(key, testInstance, clazz)
      val res = helper.datastoreEntityToInstance[TestClass](entity, clazz)
      res shouldEqual testInstance
    }

    "convert object of class with id of String type" in {
      val testObj = TestClassWithStringId()
      val key = Key(CloudKey.newBuilder("test", "TestClassWithStringId", "test").setName(testObj.id).build())
      val clazz = classOf[TestClassWithStringId]
      val entity = helper.instanceToDatastoreEntity(key, testObj, clazz)
      val res = helper.datastoreEntityToInstance[TestClassWithStringId](entity, clazz)
      res shouldEqual testObj
    }

    "create default instance of any class with at least one public constructor" in {
      helper.createDefaultInstance[TestClass1](classOf[TestClass1]) shouldEqual TestClass1(0L, "")
    }

    "convert instance with Some to datastore entity" in {
      val key = Key(CloudKey.newBuilder("test", "TestClass2", "test").build())
      val entity = helper.instanceToDatastoreEntity[TestClass2](key, TestClass2(1, Some("some")), classOf[TestClass2])
      entity.getNames.size() shouldEqual 1
      entity.getString("name") shouldEqual "some"
    }

    "convert instance with None to datastore entity" in {
      val key = Key(CloudKey.newBuilder("test", "TestClass2", "test").build())
      val entity = helper.instanceToDatastoreEntity[TestClass2](key, TestClass2(1, None), classOf[TestClass2])
      entity.getNames.size() shouldEqual 1
      entity.isNull("name") shouldEqual true
    }

    "convert datastore instance to class with Some" in {
      val instance = TestClass2(1, Some("test"))
      val key = Key(CloudKey.newBuilder("test", "TestClass", "test").setId(instance.id).build())
      val clazz = classOf[TestClass2]
      val entity = helper.instanceToDatastoreEntity(key, instance, clazz)
      val res = helper.datastoreEntityToInstance[TestClass2](entity, clazz)
      res shouldEqual instance
    }

    "convert datastore instance to class with a sequence of objects" in {
      val instance = TestClass3(
        1,
        "test",
        Seq(NestedClass(2, "test2"), NestedClass(3, "test3")),
        Seq(TestClass1(3, "test4"), TestClass1(4, "test5"))
      )
      val key = Key(CloudKey.newBuilder("test", "TestClass", "test").setId(instance.id).build())
      val clazz = classOf[TestClass3]
      val entity = helper.instanceToDatastoreEntity(key, instance, clazz)
      val res = helper.datastoreEntityToInstance[TestClass3](entity, clazz)
      res shouldEqual instance
    }

    "convert datastore instance to class with different types" in {
      val instance = TestClass4(1, Some(11.2), Some(12.1F), true)
      val key = Key(CloudKey.newBuilder("test", "TestClass", "test").setId(instance.id).build())
      val clazz = classOf[TestClass4]
      val entity = helper.instanceToDatastoreEntity(key, instance, clazz)
      val res = helper.datastoreEntityToInstance[TestClass4](entity, clazz)
      res shouldEqual instance
    }

    "convert datastore instance to class with optional ID" in {
      val instance = TestClass5(Some(11), "test", Some(true))
      val key = Key(CloudKey.newBuilder("test", "TestClass5", "test").setId(instance.id.getOrElse(0)).build())
      val clazz = classOf[TestClass5]
      val entity = helper.instanceToDatastoreEntity(key, instance, clazz)
      val res = helper.datastoreEntityToInstance[TestClass5](entity, clazz)
      res shouldEqual instance
    }
  }
}

case class TestClassWithStringId(id: String = "testId", size: Int = 1) extends BaseEntity
case class TestClass1(id: Long, name: String) extends BaseEntity
case class TestClass2(id: Long, name: Option[String]) extends BaseEntity
case class NestedClass(idx: Long, name: String)
case class TestClass3(
                       id: Long,
                       name: String,
                       options: Seq[NestedClass],
                       tests: Seq[TestClass1]
                     ) extends BaseEntity
case class TestClass4(id: Long, price: Option[Double], discount: Option[Float], onSale: Boolean) extends BaseEntity
case class TestClass5(
                       id: Option[Long],
                       name: String,
                       status: Option[Boolean]
                     ) extends BaseEntity