# Mixer Style Guide    
# ![scala](https://cdn6.aptoide.com/imgs/3/a/b/3abf4156173ca399f0bd8efad83d60b4_icon.png?w=128) @ ![cityblock](https://miro.medium.com/fit/c/128/128/1*ZjCEu2MDmMZ8GdA9UIDxvw.png)



The data-eng team at Cityblock are big fans of Scala - in particular _good_ Scala.
You may be asking, '**What** is _good_ Scala?' We're glad you asked because we wrote up a guide all about it!

## On the shoulders of giants
Scala at Cityblock is written primarily for our [Scio](https://spotify.github.io/scio/) applications, therefore, it only made sense to adopt [their style guide](https://github.com/spotify/scio/wiki/Style-Guide).
This guide itself admits it took on a majority of its direction from the [Databricks Scala Guide](https://github.com/databricks/scala-style-guide).


## Other guides? I came here for the _`mixer`_ guide. Where is it?!

Glad you asked! We currently have a lot of work to do on this end but have started with using three primary tools to ensure that our sala
style is consistent across the code base (these can be found in the scio style guide mentioned above).

- Scalafmt
- Scala-style
- Scalafix

### Scalafmt

1. To implement these changes in intellij, navigate to `Intellij IDEA -> Preferences -> Plugins` and install the Scalafmt plugin.. 
1. Now navigate to `Intellij IDEA -> Preferences -> Editor -> Scala` and choose the formatter to be `scalafmt`. **UNTICK** mark the box that states
`Reformat on file save`. Also, **UNTICK** `Use Intellij formatter for code range formatting`. This is because there seems to be a conflict between
 the Scalafmt plugin and the Intellij default. Lastly, (just for safe measure), change the scalafmt path to `~/Code/mixer/scio-jobs/.scalafmt.conf`
1. Next navigate to `Intellij IDEA -> Preferences -> Tools -> Scalafmt` and set the scalafmt path to `.scalafmt.conf`
1. Finally, navigate to `Intellij IDEA -> Preferences -> Editor > General > Ensure line feed at file end on Save` and click yes.

Once all the above steps are complete, you should be all set with scalafmt. If you wish to format code from the terminal, you can always run
`scala fmt` and find that sbt also formats the code.

### Scala-style

The Scala-style is automatically already set for Intellij and there is nothing for you to do. Hooray!! To verify, you can navigate to 
`Intellij IDEA -> Preferences -> Inspections -> Code Style -> Scala -> Scala style inspection` and make sure that the box is ticked. The 
scala style should follow the rules in scalastyle-config.xml.

In order to enforce the rules, run `sbt scalastyle`.

### Scalafix

Scalafix is a linting tool with a whole bunch of cool tools. Currently, we are only using the two features (removing unused imports and prevent implicit leaks).
Over time we will add more rules and even add as part of our CI/CD pipeline.
To look at the rules, you can navigate to the `.scalafix.conf` file. To run the rules on the project, simply run 
`sbt scalafix` and let it clean up your code.

## TL;DR
Stuck on something we're not capturing? Consult the guides in the order listed below:
<br>
**[Cityblock](#cityblock-specific) → [Scio](https://github.com/spotify/scio/wiki/Style-Guide) → [Databricks](https://github.com/databricks/scala-style-guide)**


When writing code, always keep the following principles in mind:
* **Correctness, then clarity**
* **Readability over terseness**

## Cityblock specific

TODO: no styles yet... :(
