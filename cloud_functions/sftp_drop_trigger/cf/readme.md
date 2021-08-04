# Summary

# Quickstart
```py
from cf.define import *
from cf.composer.dag_triggers import DagTriggerEffect
from cityblock import cloud_composer
cf = fn.on_gcs_blob(
    (ignore, ext(".pgp")),  # TODO: get decryption function working
    (match, prefix.filename("new_drop"), 
    DagTriggerEffect(cluster=cloud_composer.prod, dag_id="some_dag"))
) # triggers some_dag when new file is located at somewhere/random/new_drop_something.txt

```

# Common Termiology 
- GCF - Google Cloud Function
- Match - some kind of boolean test upon incoming object
- Condition - either 
    - "ignore" as a command, and a match test
    - "match" as a command, a match test, and some kind of side effect if the match test is true
- Catalog - an object that should autocomplete to a set of predefined choices
- Event - an input to a GCF. used interchangably in this module with "input", because all inputs to GCF are the result of events. 


# how to make new function
- there are no plans to do this for cityblock dmg as of 2021-07-16 but the goal is to write cf, and all future modules we write, as cleanly designed as possible, and separating the platform-specific vs organization-specific is usually an ideal that results in this. 

# How to add a new partner
# HOw to add a new dag trigger

# Notes for Contributing
- should not be cityblock specifc 
- exception to this rule is to provide utilites for using non-cityblock cf things for cityblock concepts e.g. load_to_silver matchers, collections of partner-orientated condition sets