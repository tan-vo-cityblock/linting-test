
# Coding in scala

## Setting up a Jupyter Environment

This section is meant to give you a small primer on scala and some of the cool stuff you can do. Part of this section assumes that you are
somewhat familiar with using a jupyter notebook and if not, don't fret, you'll be fine as we walk you through the steps. *NOTE*: If you don't have
anaconda installed, go back to chapter 1 and follow the instructions to install it. Once done, you can proceed forwards.

First open the terminal and run the commands 

```shell script
pip install --upgrade "ipython[all]"
SCALA_VERSION=2.12.8 ALMOND_VERSION=0.9.0
cd ~/.
curl -Lo coursier https://git.io/coursier-cli
chmod +x coursier

./coursier bootstrap \
    -r jitpack \
    -i user -I user:sh.almond:scala-kernel-api_$SCALA_VERSION:$ALMOND_VERSION \
    sh.almond:scala-kernel_$SCALA_VERSION:$ALMOND_VERSION \
    -o almond

./almond --install
```

Now to verify run the command

```shell script
jupyter kernelspec list
```

This should list scala as a kernel option. Once you are satified with the kernel, you can now remove the almond launcher

```shell script
rm -f almond
```

Now, navigate back to this directory and open a new scala kernel by first opening a jupyter notebook

```shell script
jupyter-notebook &
``` 

In this section, we will start building our own data pipeline which will be a simpler version of our current data pipeline. For the most part we will be
coding in Scala and will be using Scio (which is a wrapper on top of Apache Beam) to construct our data pipelines on Google Cloud's dataflow
(which is the runner we use).

References: 
https://almond.sh/docs/quick-start-install

## Learning Scala using an interactive notebook

Scala can seem like a daunting language to many programmers not familiar to JVM based languages. However, Scala was designed to be a simple language 
that introduced syntactic sugar and leaned more towards functional approaches. What makes scala powerful is that it is still a full and rich object oriented language
and supports several different data structures and design pattern implementations.

That being said, we will not use anything complex and in terms of data structures will stick to using Lists and Maps (which is defined as a dictionary in python).
We will use immutable objects wherever possible (which is almost always) and so you do not need to worry about state. If this last sentence didn't make sense, that's
fine since we will address this at a later time. We intend to demo scala over a notebook to allow the user a better interface when practicing writing code.

It is almost always the case that when we need to code, we need to install dependencies. In the jupyter-notebook, we will begin with installing a simple csv parser.
This parser is not the one we currently use but is pretty nice when using it for the first time. Let us go ahead and download the dependency with the following command,
 
```scala
// Install dependencies here first
interp.load.ivy( "com.github.melrief" %% "purecsv" % "0.1.1")
```

You will see a progress download bar and find that you are able to successfully download the package. Let us now import classes from our downloaded packages


```scala
// import dependencies
import purecsv.safe.CSVReader
```

Now that we were able to import that, let us now define a class that will hold data info for us.

```scala
// Create a class that will hold your data object
case class Student(id: Int,
                   first_name: String,
                   last_name: String,
                   dob: String)
```

Just for completeness, let us define a function (aka instance method) to illustrate what our class can do

```scala
case class Student(id: Int,
                   first_name: String,
                   last_name: String,
                   dob: String) = {
  def getFullName(): String = {
    val fullName: String = first_name + " " + last_name
    return fullName
  }
}
```

NOTE: the method we defined is NOT the scala way but was written this way for easier reading. As you code, you will start to grasp good practices.
Now before we can continue, we would like to be able to read the file before we can parse it. For this, we can perform the following:

```scala
import scala.io.Source
val lines: List[String] = Source.fromFile("/Users/<username>/students.txt")
                                .getLines()
                                .toList()
```

With that we are now able to access the lines. Notice that the lines also include the header. This is not something we would want to map into
our Student class. Let us go ahead and filter the line with:

```scala
val header: String = "ID,First Name,Last Name,Date of Birth"

// Filter the header out from the lines
lines
 .filter(line => line != header)
```

Hurray! We were able to remove the header. But we're still not quite at our goal. We have yet to transform CSV Strings into a Student Object.
Let us go on ahead and use the open source parser we imported earlier

```scala
// Now that we filtered the header out, we want to parse the csv line
// and insert the fields into our datamodel. For this, we can use an
// open soure csv parser

lines
 .filter(line => line != header)
 .map(row => CSVReader[CrossWalk].readCSVFromString(row ,'|'))
```

Great! We now have the lines converted into a Student Object but sadly, we're not quite there. We now have a List of lists. This looks like a job for...
FlatLand, I mean FlatMap!
```scala
lines
 .filter(line => line != header)
 .flatMap(row => CSVReader[CrossWalk].readCSVFromString(row ,'|'))
```

We're almost there! We just now need to access the contents wrapped in Success. We can do this by using the simple get method. Let's try this one last time

```scala
import scala.util.Success

lines
 .filter(line => line != header)
 .flatMap(row => CSVReader[CrossWalk].readCSVFromString(row ,'|'))
 .map(success => success.get())
```

Success! We were able to parse the CSV lines into a Student Class object.
If you're not able to view the contents of the list, you can always run:

```scala
import scala.util.Success

val students: List[Student] = lines.filter(line => line != header)
                                   .flatMap(row => CSVReader[CrossWalk].readCSVFromString(row ,'|'))
                                   .map(success => success.get())

println(students)
```

### Assigment:

Take all the code we wrote and write a simple scala class that parses the file. Try to break out Utility functions where you can and try to make the code flow
like we did above.

```scala
/* Boiler Template Meant to parse student file */

// imports
import scala.util.Source

object StudentCsvParser = {

  // No need to pass the file as command line args
  val filename: String = "/Users/haris/abc.txt"

  // Entry point to program execution
  def main(cmdLineArgs: Array[String]) = {
    // Your code here
  }
}
```

*BONUS:* Pass the filename as part of command line args to your program. *Also*, introduce a try-catch where you handle IO.