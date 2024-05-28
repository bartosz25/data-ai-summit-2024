# DAIS2024: Unit tests challenges - Apache Spark Structured Streaming

The repository stores all code snippets used in my Data+AI Summit 2024 talk:
[https://www.databricks.com/dataaisummit/session/unit-tests-how-overcome-challenges-structured-streaming](https://www.databricks.com/dataaisummit/session/unit-tests-how-overcome-challenges-structured-streaming)

The business logic implements a simple sessionization pipeline. The choice was driven by a quite wide Apache Spark Structured Streaming coverage of a sessionization pipeline. It covers the transformations, triggers, state management, and output modes, which provides a perfect overview of what Apache Spark Structured Streaming is capable of. 

You can go with Python (`python-project`) or Scala (`scala-project`) example. You can play with the code in two ways:

* either see the running application; it requires starting the Docker containers with Apache Kafka and the data generator by running
```
cd docker
docker-compose down --volumes; docker-compose up
```

* or directly see the tests; for that you can run them from CLI or directly from your IDE

## Linkable resources mentioned in the talk's summary
* Streaming: The world beyond batch, [part 1](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/), [part 2](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) by Tyler Akidau
* [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) by  Tyler Akidau, Slava Chernyak, Reuven Lax
* [Learn to Efficiently Test ETL Pipelines](https://www.youtube.com/watch?v=uzVewG8M6r0) by Jacqueline Bilston
* [Productizing Structured Streaming Jobs](https://www.youtube.com/watch?v=uP9bpaNvrvM) by Burak Yavuz
* [My 25 Laws of Test Driven Development](https://infoshare.pl/conference/agenda/#talk757-5) by Dennis Doomen

## Great Expectations setup
Before running the tests, run the Great Expectations up with:
```
python setup/expectations_preparator.py
```

After running the command you should see a new _great_expectations_spec_ directory:

```
$ tree great_expectations_spec/  --charset unicode
great_expectations_spec/
`-- gx
    |-- checkpoints
    |-- expectations
    |   |-- devices_expectations.json
    |   `-- visits_expectations.json
    |-- great_expectations.yml
    |-- plugins
    |   `-- custom_data_docs
    |       |-- renderers
    |       |-- styles
    |       |   `-- data_docs_custom_styles.css
    |       `-- views
    |-- profilers
    `-- uncommitted
        |-- config_variables.yml
        |-- data_docs
        `-- validations

12 directories, 5 files
```

Next, uncomment this line in the `test/conftest.py`:
```
    #request.addfinalizer(validate_test_datasets)
```
