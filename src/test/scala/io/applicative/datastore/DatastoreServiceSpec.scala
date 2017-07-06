package io.applicative.datastore

import com.google.cloud.datastore.{IncompleteKey, KeyFactory, Datastore => CloudDataStore, Key => CloudKey}
import io.applicative.datastore.util.reflection.{TestClass}
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import scala.concurrent.ExecutionContext.Implicits.global
import io.applicative.datastore.{BaseEntity, Key}


class DatastoreServiceSpec extends Specification with Mockito {
  private val cloudDataStore = mock[CloudDataStore]
  private val dataStoreService = DatastoreService
  dataStoreService.setCloudDataStore(cloudDataStore)
  private val testInstance = TestClass()
  type EE = ExecutionEnv

  "DatastoreService should" should {

    "create a new key with specified Kind" in { implicit ee: EE =>
      cloudDataStore.newKeyFactory() returns mockKeyFactory()
      val mk = mockKey("TestClass", testInstance.id)
      cloudDataStore.allocateId(any[IncompleteKey]) returns mk
      val key = dataStoreService.newKey[TestClass]()
      key must beEqualTo(Key(mk)).await
    }

    "create datastore record for Long key" in { implicit ee: EE =>
      cloudDataStore.newKeyFactory() returns mockKeyFactory()
      val mk = mockKey("TestClass1", testInstance.id)
      cloudDataStore.allocateId(any[IncompleteKey]) returns mk
      val instance = TestClass1(1, "test")
      val entity = dataStoreService.add[TestClass1](instance)
      val mockInstance = TestClass1(1, "test")
      entity must beEqualTo(mockInstance).await
    }

    "create datastore record for optional key" in { implicit ee: EE =>
      cloudDataStore.newKeyFactory() returns mockKeyFactory()
      val mk = mockKey("TestClass5", testInstance.id)
      cloudDataStore.allocateId(any[IncompleteKey]) returns mk
      val instance = TestClass5(None, "test", None)
      val entity = dataStoreService.add[TestClass5](instance)
      val mockInstance = TestClass5(Some(9223372036854775807L), "test", None)
      entity must beEqualTo(mockInstance).await
    }
  }

  private def mockKeyFactory() = {
    new KeyFactory("mock")
  }

  private def mockKey(kind: String, id: Long) = {
    CloudKey.newBuilder("test", kind, id).build()
  }

}
case class TestClass1(id: Long, name: String) extends BaseEntity
case class TestClass5(
                       id: Option[Long],
                       name: String,
                       status: Option[Boolean]
                     ) extends BaseEntity