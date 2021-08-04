# Scio Jobs

## Overview
Various strategies when getting issues for running Scio jobs locally

### Reading Member Index failures
When running a Scio job that needs to read from the Member index, it will fail if your IP address is not allowed in the
 firewall rules.

#### If this is the problem...
After running `LoadToSilverRunner` in your terminal, you will see an error that starts like this:
```
[error] (run-main-0) java.lang.Exception: Failed to read Member Index for partner emblem:  ResponseError(UnknownResponseError(<!DOCTYPE html>
```

You'll eventually see these lines too at the bottom of the trace (after HTML):
``` 
[error]   <p>Access is forbidden.  <ins>That’s all we know.</ins> 
```

#### Steps overview
Are you working from a new IP address? If so, it probably needs to be allowed in the firewall rules for the Member
 Service. Confirm [your IP address](https://www.whatsmyip.org/) is present in this list:
```bash
gcloud --project=cbh-member-service-prod app firewall-rules list
```

**NOTE: If you are unable to run these commands, reach out to Platform team in**
 [#platform-support](https://cityblockhealth.slack.com/archives/CAQNY165D)

If it is not present, run the following:
```bash
UNUSED_PRIORITY_NUM=... # pick something in the 20XX range
IP_ADDRESS=... # https://www.whatsmyip.org/
DESCRIPTION=... #short sentence on physical location ie. Ben's Brooklyn home
gcloud --project=cbh-member-service-prod app firewall-rules create $UNUSED_PRIORITY_NUM --source-range=$IP_ADDRESS --action=ALLOW --description=$DESCRIPTION
# Should output: Created [$UNUSED_PRIORITY_NUM].
```

#### Check resolution
You can test Member Service connectivity:
```
curl https://cbh-member-service-prod.appspot.com/keep_warm
# output should be something along the lines of 'app active!'
```

You should now be able to run `LoadToSilverRunner` without getting the error message above.
