
# Data Pipelines using Scio

In this section, we will start building our own data pipeline which will be a simpler version of our current data pipeline. For the most part we will be
coding in Scala and will be using Scio (which is a wrapper on top of Apache Beam) to construct our data pipelines on Google Cloud's dataflow
(which is the runner we use).

References: 
https://beam.apache.org/documentation/programming-guide/
https://spotify.github.io/scio/index.html
https://spotify.github.io/scio/Getting-Started.html

From this point on, we'll begin building things from scratch in order to provide you with more familiarity with tools over time. In this directory,
run the following commands:

```bash
sbt new spotify/scio.g8
```

You will be prompted to set the scio job name and the company name. Enter the following:

```shell script
name [scio job]: starter pipeline
organization [example]: cityblock
```

This will effectively create a new project with some predefined dependencies and package structure to work with. You can find the dependencies at `build.sbt`
which you are encouraged to scan through. You'll also have generated a README.md so go ahead, read through it and feel free to populate it with your name
and anything you feel may be helpful as you develop this mini-project.

Now to test that this works, lets run

```shell script
sbt shell
runMain cityblock.WordCount --output=wordcount-results
:q
```

Notice you'll find a new folder called `output-results`. Navigate to this folder and type `cat part-* | sort | more` and you'll find that you just did a quick
word count for Shakespeare's King Lear. To see the most often used words, feel free to run the shell command `sort -n -t ':' -k2 part-0000*`

This is a skeleton for you to start working with as we begin building data pipelines. In the next section, we'll look into building your pipeline a bit more and
explain the mechanics and what happens under the hood.