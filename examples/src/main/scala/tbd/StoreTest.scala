package tbd.examples

import akka.actor.ActorSystem
import akka.serialization._
import com.sleepycat.je.{DatabaseConfig, DatabaseException, Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship._
import java.io._
import scala.collection.mutable.Set
import scala.pickling._
import binary._

@Entity
class TestEntity {
  @PrimaryKey
  var key: String = null

  var value: Array[Byte] = null
}

@Entity
class TestEntity2 {
  @PrimaryKey
  var key: String = null

  var value: Set[Int] = null
}

class StoreTest {
  private var environment: Environment = null
  private var store: EntityStore = null

  private val envConfig = new EnvironmentConfig()
  envConfig.setAllowCreate(true)
  val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  val random = new scala.util.Random()
  private val envHome = new File("/tmp/tbd_berkeleydb" + random.nextInt())
  envHome.mkdir()

  try {
    // Open the environment and entity store
    environment = new Environment(envHome, envConfig)
    store = new EntityStore(environment, "EntityStore", storeConfig)
  } catch {
    case fnfe: FileNotFoundException => {
      System.err.println("setup(): " + fnfe.toString())
      System.exit(-1)
    }
  }

  val pIdx = store.getPrimaryIndex(classOf[String], classOf[TestEntity])
  val pIdx2 = store.getPrimaryIndex(classOf[String], classOf[TestEntity2])

  val original = Set(0)

  val count = 100

  for (i <- 1 to 10000) {
    original += i
  }

  def runJava() {
    // write
    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(original)

    val entity = new TestEntity()
    entity.key = original.toString
    entity.value = byteOutput.toByteArray
    pIdx.put(entity)
    val byteArray = pIdx.get(entity.key).value

    // read
    val byteInput = new ByteArrayInputStream(byteArray)
    val objectInput = new ObjectInputStream(byteInput)
    val obj = objectInput.readObject()

    assert(original == obj)
  }

  def runScala() {
    val pickle = original.pickle

    val entity = new TestEntity()
    entity.key = original.toString
    entity.value = pickle.value
    pIdx.put(entity)
    val byteArray = pIdx.get(entity.key).value

    val obj = byteArray.unpickle[scala.collection.mutable.Set[String]]
    assert(obj == original)
  }

  val system = ActorSystem("example")
  val serialization = SerializationExtension(system)
  def runAkka() {
    val serializer = serialization.serializerFor(original.getClass)

    val entity = new TestEntity()
    entity.key = original.toString
    entity.value = serializer.toBinary(original)
    pIdx.put(entity)
    val byteArray = pIdx.get(entity.key).value

    val obj = serializer.fromBinary(byteArray, manifest = None)
    assert(obj == original)
  }

  def runDB() {
    val entity = new TestEntity2()
    entity.key = original.toString
    entity.value = original
    pIdx2.put(entity)
    pIdx.get(entity.key)
  }

  val byteOutput = new ByteArrayOutputStream()
  val objectOutput = new ObjectOutputStream(byteOutput)
  objectOutput.writeObject(original)
  val db2ByteArray = byteOutput.toByteArray
  def runDB2() {
    val entity = new TestEntity()
    entity.key = original.toString
    entity.value = db2ByteArray
    pIdx.put(entity)
    pIdx.get(entity.key)
  }

  def run(func: () => Unit, desc: String = "") {
    val before = System.currentTimeMillis()

    for (i <- 1 to count) {
      func()
    }

    if (desc != "") {
      println(desc + "\t" + (System.currentTimeMillis() - before))
    }
  }

  def main() {
    // Warmup
    run(runAkka)
    //run(runDB)
    run(runJava)
    run(runScala)
    run(runDB2)

    //run(runDB, "DB:")
    run(runJava, "Java:")
    run(runScala, "Scala:")
    run(runDB2, "DB2:")
    run(runAkka, "Akka:")

  }
}
