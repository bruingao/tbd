package tbd.examples

class TestTBD {
}

class TestMod[T] {
  def read[U](reader: T => TestChangeable[U])(implicit tbd: TestTBD) {}

  def ->[U](reader: T => TestChangeable[U])(implicit tbd: TestTBD) {}
}

class TestChangeable[T] {
}

object Test {
  def write[T](value: T): TestChangeable[T] = ???

  def mod[T](init: => T) = ???

  def par(one: Adjust, two: Adjust) {}

  def main(args: Array[String]) {
    (new StoreTest()).main()
  }
}

trait Adjust {
  def run()(implicit tbd: TestTBD)
}

class TestMemoizer[T] {
  def apply(args: Any*)(func: => T): T = ???
}

class Test[T, V](
    var value: (T, V),
    val next: TestMod[Test[T, V]]
  ) extends Adjust {
  import Test._

  val mod1 = new TestMod()

  def run()(implicit tbd: TestTBD) {

    mod1 read (value1 => {
      write(1)
    })

    mod1 -> (value1 => {
      write(1)
    })

    par(new Adjust(){
      def run()(implicit tbd: TestTBD) {
      
      }
    }, new Adjust() {
      def run()(implicit tbd: TestTBD) {
      }
    })
  }

  def map[U, Q](
      f: (TestTBD, (T, V)) => (U, Q),
      memo: TestMemoizer[TestChangeable[Test[U, Q]]])
      (implicit tbd: TestTBD)
        : TestChangeable[Test[U, Q]] = {
    val newNext = memo(next) {
      mod {
	next -> (nextValue => {
          if (nextValue != null) {
            next.map(f, memo)
          } else {
            write[Test[U, Q]](null)
          }
	})
      }
    }

    write(new Test[U, Q](f(tbd, value), newNext))
  }
}
