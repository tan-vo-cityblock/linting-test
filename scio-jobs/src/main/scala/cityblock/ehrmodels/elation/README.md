# Elation

## Raison d'être

This package is implemented in scala and meant to provide an interface to interact with Elation objects and resources via their 
[web Api](https://docs.elationhealth.com/reference). As of now, this has been implemented primarily on the datamodel 
[Patient object](https://docs.elationhealth.com/reference#patient).

This guide is meant to provide a walk through of the implementation in order to provide easier access to scale this project.
The current implementation is using [STTP](https://sttp.readthedocs.io/en/latest/) in order to make http requests to the elation API. Elation 
offers several different backends to use which can be found [here](https://sttp.readthedocs.io/en/latest/backends/summary.html). 
The preferred backend to use is the `HttpURLConnectionBackend`. For [testing purposes](https://sttp.readthedocs.io/en/latest/testing.html) the
preferred backend is to use `SttpBackendStub.synchronous`. Examples of using the `HttpURLConnectionBackend` is provided below.

All server communication is hidden from the Elation Objects. 


## Explanation

#### Authentication

Under the current implementation, token generation for Authorization occurs in the AuthToken class which grabs the password, client id etc. from the `src/main/resources/application.conf`file. This class is marked in the .gitignore file and will never be pushed up to the repository. For convenient's sake, you make look at the `src/main/resources/applicationExample.conf`file in order to understand which fields need to be populated and how.


#### Requests 

All the request logic and error handling is done in Requests.scala and is not meant to be changed. The Requests.scala class is 
meant to only be accessible to the Elation.scala class.

Elation.scala is where all the CRUD operations to the Elation API exists. Under the circumstance that you would want to
implememt a new DataModel, you would simply add a Read/Write method within the companion object. 
The Elation.scala class is privately scoped to only be used within the elation project. Meaning that all datamodels must
reside in this heirachy.

*NOTE*: All creation and deletion operations have been removed from this implementation and only read capabilities are available. In order to 
read, update or delete any information stored in Elation, please reference documentation on the Member Service.


#### DataModels

In order to get a quick look at the datamodels available, You can try this example to read a Patient Object from elation. 


```scala 
import cityblock.ehrmodels.elation.datamodel.patient._
import com.softwaremill.sttp.HttpURLConnectionBackend

implicit val backend = implicit val backend = HttpURLConnectionBackend()
val pat: Patient = Patient.findById("SOMEUNIQUEID")
```

We could use some pretty json printing by expanding this example.

```scala 
import cityblock.ehrmodels.elation.datamodel.patient._
import com.softwaremill.sttp.HttpURLConnectionBackend

import io.circe.generic.auto._
import io.circe.syntax._

implicit val backend = implicit val backend = HttpURLConnectionBackend()
val pat: Patient = Patient.findById("SOMEUNIQUEID")

println(pat.right.get.asJson)
```


#### Errors

Currently, there are only a handful of errors which can all be found in utilities/backend/ApiError.scala
