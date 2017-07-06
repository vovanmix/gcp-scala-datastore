package io.applicative.datastore.util.reflection

import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset, ZonedDateTime}
import java.util.Date

import com.google.cloud.datastore._
import io.applicative.datastore.Key
import io.applicative.datastore.exception.{MissedTypeParameterException, UnsupportedFieldTypeException}
import io.applicative.datastore.util.DateTimeHelper

import scala.reflect.{ClassTag, api, classTag}
import com.google.cloud.datastore.{Key => GCKey}

import scala.reflect.runtime.universe._
import scala.util.Try
import scala.collection.JavaConverters._

private[datastore] trait ReflectionHelper extends DateTimeHelper {

  private val ByteClassName = getClassName[Byte]()
  private val IntClassName = getClassName[Int]()
  private val LongClassName = getClassName[Long]()
  private val FloatClassName = getClassName[Float]()
  private val DoubleClassName = getClassName[Double]()
  private val StringClassName = getClassName[String]()
  private val JavaUtilDateClassName = getClassName[Date]()
  private val BooleanClassName = getClassName[Boolean]()
  private val DatastoreDateTimeClassName = getClassName[DateTime]()
  private val DatastoreLatLongClassName = getClassName[LatLng]()
  private val DatastoreBlobClassName = getClassName[Blob]()
  private val LocalDateTimeClassName = getClassName[LocalDateTime]()
  private val ZonedDateTimeClassName = getClassName[ZonedDateTime]()
  private val OffsetDateTimeClassName = getClassName[OffsetDateTime]()
  private val OptionClassName = getClassName[Option[_]]()
  private val SeqClassName = getClassName[scala.collection.immutable.Seq[_]]()

  private[datastore] def extractRuntimeClass[E: ClassTag](): RuntimeClass = {
    val runtimeClass = classTag[E].runtimeClass
    if (runtimeClass == classOf[Nothing]) {
      throw MissedTypeParameterException()
    }
    runtimeClass
  }

  private[datastore] def instanceToDatastoreEntity[E](key: Key, classInstance: E, clazz: Class[_]): Entity = {
    var builder = Entity.newBuilder(key.key)
    clazz.getDeclaredFields
      .filterNot(_.isSynthetic)
      // Take all fields except the first one assuming it is an ID field, which is already encapsulated in the Key
      .tail
      .map(f => {
        f.setAccessible(true)
        Field(f.getName, f.get(classInstance))
      })
      .foreach {
        case Field(name, Some(value: Any)) => builder = setValue(Field(name, value), builder)
        case Field(name, None) => builder.setNull(name)
        case field => builder = setValue(field, builder)
      }
    builder.build()
  }

  private def mapInstanceTODatastoreEntity[E: TypeTag : ClassTag](classInstance: E): EntityValue = {
    val clazz: Class[_] = classInstance.getClass
    var builder = FullEntity.newBuilder()
    clazz.getDeclaredFields
      .filterNot(_.isSynthetic)
      .map(f => {
        f.setAccessible(true)
        Field(f.getName, f.get(classInstance))
      })
      .map {
        case Field(name, Some(value: Any)) => setValue(Field(name, value), builder)
        case Field(name, None) => builder.setNull(name)
        case field => setValue(field, builder)
      }
    val newEntity: FullEntity[IncompleteKey] = builder.build()

    new EntityValue(newEntity)
  }

  private def setFieldValue(v: Any): Any = {
    v match {
      case value: Boolean => value
      case value: Byte => value
      case value: Int => value
      case value: Long => value
      case value: Float => value
      case value: Double => value
      case value: String => value
      case value: Date => toMilliSeconds(value.asInstanceOf[Date])
      case value: DateTime => value
      case value: LocalDateTime => formatLocalDateTime(value.asInstanceOf[LocalDateTime])
      case value: OffsetDateTime => formatOffsetDateTime(value.asInstanceOf[OffsetDateTime])
      case value: ZonedDateTime => formatZonedDateTime(value.asInstanceOf[ZonedDateTime])
      case value: LatLng => value
      case value: Blob => value
      case value: Seq[_] => value.asInstanceOf[Seq[_]].map(setFieldValue)
      case null => null
      case value => mapInstanceTODatastoreEntity(value)
    }
  }

  private def setValue(field: Field[_], builder: Entity.Builder): Entity.Builder = {
    field match {
      case Field(name, value: Boolean) => builder.set(name, value)
      case Field(name, value: Byte) => builder.set(name, value)
      case Field(name, value: Int) => builder.set(name, value)
      case Field(name, value: Long) => builder.set(name, value)
      case Field(name, value: Float) => builder.set(name, value)
      case Field(name, value: Double) => builder.set(name, value)
      case Field(name, value: String) => builder.set(name, value)
      case Field(name, value: Date) => builder.set(name, toMilliSeconds(value))
      case Field(name, value: DateTime) => builder.set(name, value)
      case Field(name, value: LocalDateTime) => builder.set(name, formatLocalDateTime(value))
      case Field(name, value: OffsetDateTime) => builder.set(name, formatOffsetDateTime(value))
      case Field(name, value: ZonedDateTime) => builder.set(name, formatZonedDateTime(value))
      case Field(name, value: LatLng) => builder.set(name, value)
      case Field(name, value: Blob) => builder.set(name, value)
      case Field(name, null) => builder.setNull(name)
      case Field(name, value: Seq[String]) =>
        val seq = value.asInstanceOf[Seq[String]].map(setFieldValue)
        val list = new java.util.ArrayList(seq.toList.asJava)
        builder.set(name, list.asInstanceOf[java.util.List[StringValue]])
//        builder.set(name, value.asInstanceOf[Seq[_]].map(setFieldValue).toBuffer.asInstanceOf[java.util.List[StringValue]])
      case Field(name, value) => builder.set(name, mapInstanceTODatastoreEntity(value))
    }
  }

  // TODO: make it a single method
  private def setValue(field: Field[_], builder: FullEntity.Builder[IncompleteKey]) = {
    field match {
      case Field(name, value: Boolean) => builder.set(name, value)
      case Field(name, value: Byte) => builder.set(name, value)
      case Field(name, value: Int) => builder.set(name, value)
      case Field(name, value: Long) => builder.set(name, value)
      case Field(name, value: Float) => builder.set(name, value)
      case Field(name, value: Double) => builder.set(name, value)
      case Field(name, value: String) => builder.set(name, value)
      case Field(name, value: Date) => builder.set(name, toMilliSeconds(value))
      case Field(name, value: DateTime) => builder.set(name, value)
      case Field(name, value: LocalDateTime) => builder.set(name, formatLocalDateTime(value))
      case Field(name, value: OffsetDateTime) => builder.set(name, formatOffsetDateTime(value))
      case Field(name, value: ZonedDateTime) => builder.set(name, formatZonedDateTime(value))
      case Field(name, value: LatLng) => builder.set(name, value)
      case Field(name, value: Blob) => builder.set(name, value)
      case Field(name, null) => builder.setNull(name)
      case Field(name, value: Seq[String]) =>
        val seq = value.asInstanceOf[Seq[String]].map(setFieldValue)
        val list = new java.util.ArrayList(seq.toList.asJava)
        builder.set(name, list.asInstanceOf[java.util.List[StringValue]])
      case Field(name, value) => builder.set(name, mapInstanceTODatastoreEntity(value))
    }
  }

  private[datastore] def datastoreEntityToInstance[E: TypeTag : ClassTag](entity: FullEntity[GCKey], clazz: Class[_]): E = {
    val defaultInstance = createDefaultInstance[E](clazz)
    setActualFieldValues(defaultInstance, entity)
    val a = defaultInstance.asInstanceOf[E]
    a
  }

  private def typeToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T]( typeTag[T].mirror.runtimeClass( typeTag[T].tpe ) )
  }

  private def getTypeTag[T](clazz: Class[_], tpe: T): TypeTag[T] = {
    val mirror = runtimeMirror(clazz.getClassLoader)
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: api.Universe with Singleton](m: api.Mirror[U]) =
        if (m eq mirror) tpe.asInstanceOf[U#Type]
        else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
    })
  }

  private def getClass(t: Type): Class[_] = scala.reflect.runtime.currentMirror.runtimeClass(t)

  private def mapDatastoreEntityToInstance(tpe: Type)(entity: FullEntity[GCKey]) = {
    val clazz = getClass(tpe)
    val typeTag: TypeTag[Any] = getTypeTag(clazz, tpe)
    val classTag: ClassTag[Any] = typeToClassTag(typeTag)
    datastoreEntityToInstance(entity, clazz)(typeTag, classTag)
  }

  private def setSeqValues(member: MethodSymbol, entity: FullEntity[GCKey], fieldName: String): Seq[Any] = {
    val ta = member.returnType.typeArgs
    val fieldClassName = ta.head.typeSymbol.fullName
    fieldClassName match {
      case ByteClassName => getList[LongValue, Long](entity, fieldName, e => e.toByte)
      case IntClassName => getList[LongValue, Long](entity, fieldName, e => e.toInt)
      case LongClassName => getList[LongValue, Long](entity, fieldName, e => e.toLong)
      case StringClassName => getList[StringValue, String](entity, fieldName)
      case FloatClassName => getList[DoubleValue, Double](entity, fieldName, e => e.toFloat)
      case DoubleClassName => getList[DoubleValue, Double](entity, fieldName, e => e.toDouble)
      case BooleanClassName => getList[BooleanValue, Boolean](entity, fieldName)
      case JavaUtilDateClassName => getList[LongValue, Long](entity, fieldName, toJavaUtilDate)
      case DatastoreDateTimeClassName => getList[DateTimeValue, DateTime](entity, fieldName)
      case LocalDateTimeClassName => getList[StringValue, String](entity, fieldName, parseLocalDateTime)
      case ZonedDateTimeClassName => getList[StringValue, String](entity, fieldName, parseZonedDateTime)
      case OffsetDateTimeClassName => getList[StringValue, String](entity, fieldName, parseOffsetDateTime)
      case DatastoreLatLongClassName => getList[LatLngValue, LatLng](entity, fieldName)
      case DatastoreBlobClassName => getList[BlobValue, Blob](entity, fieldName)
      case _ =>
        val symbol = ta.head.resultType
        getList[EntityValue, FullEntity[GCKey]](entity, fieldName, mapDatastoreEntityToInstance(symbol))
    }
  }

  private def getList[T <: Value[_], V](entity: FullEntity[GCKey], fieldName: String, f: V => Any = null): Seq[Any] = {
    val l = entity.getList[T](fieldName)
    val list: List[T] = l.toArray.toList.asInstanceOf[List[T]]
    if (f == null) list.map(e => e.get.asInstanceOf[V])
    else list.map(e => f(e.get.asInstanceOf[V]))
  }

  private def setActualFieldValues[T](a: T, entity: FullEntity[GCKey])(implicit tt: TypeTag[T], ct: ClassTag[T]): Unit = {
    tt.tpe.members.collect {
      case m if m.isMethod && m.asMethod.isCaseAccessor => m.asMethod
    } foreach { member => {
      val field = tt.mirror.reflect(a).reflectField(member)
      val fieldClassName = member.returnType.typeSymbol.fullName
      val fieldName = member.name.toString
      if (fieldName == "id" && entity.getKey != null) {
        field.set(entity.getKey.getNameOrId)
      } else {
        val value = fieldClassName match {
          case OptionClassName =>
            if (entity.isNull(fieldName)) {
              None
            } else {
              val genericClassName = member.returnType.typeArgs.head.typeSymbol.fullName
              Some(getValue(genericClassName, fieldName, entity))
            }
          case SeqClassName =>
            if (entity.isNull(fieldName)) {
              Seq()
            } else {
              setSeqValues(member, entity, fieldName)
            }
          case className =>
            getValue(className, fieldName, entity)
        }
        field.set(value)
      }
    }
    }
  }

  private def getValue(className: String, fieldName: String, entity: FullEntity[GCKey]): Any = {
    className match {
      case ByteClassName => entity.getLong(fieldName).toByte
      case IntClassName => entity.getLong(fieldName).toInt
      case LongClassName => entity.getLong(fieldName)
      case StringClassName => entity.getString(fieldName)
      case FloatClassName => Try(entity.getDouble(fieldName).toFloat).getOrElse(entity.getLong(fieldName).toFloat)
      case DoubleClassName => Try(entity.getDouble(fieldName)).getOrElse(entity.getLong(fieldName).toDouble)
      case BooleanClassName => entity.getBoolean(fieldName)
      case JavaUtilDateClassName => toJavaUtilDate(entity.getLong(fieldName))
      case DatastoreDateTimeClassName => entity.getDateTime(fieldName)
      case LocalDateTimeClassName => parseLocalDateTime(entity.getString(fieldName))
      case ZonedDateTimeClassName => parseZonedDateTime(entity.getString(fieldName))
      case OffsetDateTimeClassName => parseOffsetDateTime(entity.getString(fieldName))
      case DatastoreLatLongClassName => entity.getLatLng(fieldName)
      case DatastoreBlobClassName => entity.getBlob(fieldName)
      case otherClassName => throw UnsupportedFieldTypeException(otherClassName)
    }
  }

  private[datastore] def createDefaultInstance[E](clazz: Class[_]): E = {
    val constructor = clazz.getConstructors.head
    val params = constructor.getParameterTypes.map(cl => getClassName(cl) match {
      case ByteClassName => Byte.MinValue
      case IntClassName => Int.MinValue
      case LongClassName => 0L
      case StringClassName => ""
      case FloatClassName => 0.0F
      case DoubleClassName => 0.0
      case BooleanClassName => false
      case JavaUtilDateClassName => new Date(0)
      case DatastoreDateTimeClassName => DateTime.copyFrom(new Date(0))
      case LocalDateTimeClassName => LocalDateTime.MIN
      case ZonedDateTimeClassName => ZonedDateTime.of(1900, 1, 1, 1, 1, 1, 1, ZoneOffset.UTC)
      case OffsetDateTimeClassName => OffsetDateTime.MIN
      case DatastoreLatLongClassName => LatLng.of(0.0, 0.0)
      case DatastoreBlobClassName => Blob.copyFrom(Array[Byte]())
      case OptionClassName => None
      case SeqClassName => Seq()
      case fieldName => throw UnsupportedFieldTypeException(fieldName)
    }).map(_.asInstanceOf[Object])
    constructor.newInstance(params: _*).asInstanceOf[E]
  }

  private[datastore] def getClassName(clazz: Class[_]): String = {
    runtimeMirror(clazz.getClassLoader).classSymbol(clazz).fullName
  }

  private[datastore] def getClassName[E: TypeTag](): String = {
    typeOf[E].typeSymbol.fullName
  }
}