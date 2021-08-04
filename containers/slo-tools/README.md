# Service Level Objective (SLO) Tools
Container for utility functions and scripts related to all SLO oriented workflows

## General notes
- Aim to keep the container as lightweight in order to optimize portability. 
This is not the place to contain heavy business logic.

## Modules
### `db_mirror`
Python scripts for calculating Service Level Indicators (SLIs) for Database Mirrors
- Coverage: How much of the Database has been mirrored from source to sink
  - See `see_error_summary` method on how to get granular coverage information for a given BQ shard.
- Freshness: How recent is the Database data in the sink location

There is also a [`db_ref.json`](./db_mirror/util/db_ref.json) that contains configuration details related to the 
freshness SLI.
