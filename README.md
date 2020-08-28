# gcs-inventory-loader

Load your GCS bucket inventory into BigQuery (or stdout) fast with this tool.

## Introduction

It can be very useful to have an inventory of your GCS objects and their metadata, particularly in a powerful database like BigQuery. The GCS listing API supports filtering by prefixes, but more complex queries can't be done via API. Using a database, you can find out lots of information about the data you have in GCS, such as finding very large objects, very old or stale objects, etc.

This utility will help you bulk load an object listing to stdout, or directly into BigQuery. It can also help you keep your inventory up-to-date with the listen command.

The implementation here takes the approach of listing buckets and sending each page to a worker in a thread pool for processing and streaming into BigQuery. Throughput rates of 15s per 100,000 objects have been achieved with moderately sized (32 vCPU) virtual machines. This works out to 2 minutes and 30 seconds per million objects. Note that this throughput is _per process_ -- simply shard the bucket namespace across multiple projects to increase this throughput.

## Costs

Compute costs notwithstanding, the primary cost you'll incur for listing objects is Class A operations charges. Under most circumstances you'll get a listing with 1000 objects per page (exceptional circumstances might be... you just did a lot of deletes and the table is sparse). So cost is figured like so:

`(number of objects listed) / 1000 / 10,000 * (rate per 10,000 class A ops)`

For example, in a standard regional bucket, listing 100 million objects should cost about .5 USD:

[`(100 million) / 1000 / 10,000 * $0.05 = $0.50`](https://www.wolframalpha.com/input/?i=%28100+million%29+%2F+1000+%2F+10%2C000+*+%240.05)

## Installation

  1) Download this repository and `cd` into it.
  2) Run `pip install .`. Optionally, add the `-e` switch. This will allow you to make modifications if you'd like.

## Usage

First, configure the config file. You will find a `./default.cfg` file in the root of this repo. Each field is documented. Be sure to fill in the `PROJECT` value, and if streaming to BigQuery directly, the `DATASET_NAME` and `INVENTORY_TABLE` values.

Now, you are ready to run the command. You can get general help by just running `gcs_inventory`.

``` shell
$ gcs_inventory
Usage: gcs_inventory [OPTIONS] COMMAND [ARGS]...
```

### Streaming to BigQuery

If you've configured the config file correctly, you should be able to get your bucket inventory loaded with a simple command:

``` shell
gcs_inventory load
```

Note that by default, this will load an inventory of all objects for _all buckets_ in your project. To restrict this further, you can use the bucket list argument and/or the filter option:

``` shell
gcs_inventory load bucket1 bucket2
```

``` shell
gcs_inventory load bucket1 -p stuffICareAbout/
```

### Writing to stdout

You can also output the records in newline-delimited JSON for use with another database, or for bulk loading into BigQuery via GCS. For example:

``` shell
gcs_inventory cat > inventory.ldjson
```

You can combine this command with gsutil's ability to read from stdin for a stream directly into GCS:

``` shell
gcs_inventory cat | gsutil cp - gs://mybucket/inventory.ldjson
```

### Listening for changes

You can use the listen command to pull [PubSub object change notifications](https://cloud.google.com/storage/docs/pubsub-notifications)
from a subscription and stream them into your BigQuery table in order to keep it up to date. Configure the PUBSUB section of the
configuration file and then simply run this command:

```shell
gcs_inventory listen
```

You can rely upon the message retention of PubSub subscriptions to run this job on a scheduled basis to true-up your inventory, or just keep this utility running 24/7. If you have a very high rate of change, it is safe to run multiple listeners.

Note that deletes will be recorded by the addition of a timeDeleted value.

To set up PubSub notifications for a given bucket, use this gcloud command:

```shell
gsutil notification create -t [TOPIC_NAME] -f json gs://[BUCKET_NAME]
```

[See here](https://cloud.google.com/storage/docs/reporting-changes) for more info.

## Known Issues

- The listen and load commands may fail and lose messages on a first run if the target table is not already created. This is because table creation in BigQuery is eventually consistent, and the code to create the table doesn't quite account for that yet. In the case of the load command, you can simply retry; in the case of the listen command, *you may lose some messages*. You can avoid this by either running the load command successfully (with >0 rows) first, or simply running the listen command and exiting before it processes any messages and then waiting a minute.

## Important Disclaimer

This code is written by a Googler, but this project is not supported by Google in any way. As the `LICENSE` file says, this work is offered to you on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License.

## License

Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

## Copyright

Copyright 2020 Google LLC.
