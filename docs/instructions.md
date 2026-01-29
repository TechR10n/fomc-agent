
#### Part 1: AWS S3 & Sourcing Datasets
1. Republish [this open dataset](https://download.bls.gov/pub/time.series/pr/) in Amazon S3 and share with us a link.
    - You may run into 403 Forbidden errors as you test accessing this data. There is a way to comply with the BLS data access policies and re-gain access to fetch this data programatically - we have included some hints as to how to do this at the bottom of this README in the Q/A section.
2. Script this process so the files in the S3 bucket are kept in sync with the source when data on the website is updated, added, or deleted.
    - Don't rely on hard coded names - the script should be able to handle added or removed files.
    - Ensure the script doesn't upload the same file more than once.

#### Part 2: APIs
1. Create a script that will fetch data from [this API](https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population).
   You can read the documentation [here](https://datausa.io/about/api/).
2. Save the result of this API call as a JSON file in S3.

#### Part 3: Data Analytics
0. Load both the csv file from **Part 1** `pr.data.0.Current` and the json file from **Part 2**
   as dataframes ([Spark](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html),
                  [Pyspark](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html),
                  [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html),
                  [Koalas](https://koalas.readthedocs.io/en/latest/),
                  etc).

1. Using the dataframe from the population data API (Part 2),
   generate the mean and the standard deviation of the annual US population across the years [2013, 2018] inclusive.

2. Using the dataframe from the time-series (Part 1),
   For every series_id, find the *best year*: the year with the max/largest sum of "value" for all quarters in that year. Generate a report with each series id, the best year for that series, and the summed value for that year.
   For example, if the table had the following values:

    | series_id   | year | period | value |
    |-------------|------|--------|-------|
    | PRS30006011 | 1995 | Q01    | 1     |
    | PRS30006011 | 1995 | Q02    | 2     |
    | PRS30006011 | 1996 | Q01    | 3     |
    | PRS30006011 | 1996 | Q02    | 4     |
    | PRS30006012 | 2000 | Q01    | 0     |
    | PRS30006012 | 2000 | Q02    | 8     |
    | PRS30006012 | 2001 | Q01    | 2     |
    | PRS30006012 | 2001 | Q02    | 3     |

    the report would generate the following table:

    | series_id   | year | value |
    |-------------|------|-------|
    | PRS30006011 | 1996 | 7     |
    | PRS30006012 | 2000 | 8     |

3. Using both dataframes from Part 1 and Part 2, generate a report that will provide the `value`
   for `series_id = PRS30006032` and `period = Q01` and the `population` for that given year (if available in the population dataset).
   The below table shows an example of one row that might appear in the resulting table:

    | series_id   | year | period | value | Population |
    |-------------|------|--------|-------|------------|
    | PRS30006032 | 2018 | Q01    | 1.9   | 327167439  |

    **Hints:** when working with public datasets you sometimes might have to perform some data cleaning first.
   For example, you might find it useful to perform [trimming](https://stackoverflow.com/questions/35540974/remove-blank-space-from-data-frame-column-values-in-spark) of whitespaces before doing any filtering or joins


4. Submit your analysis, your queries, and the outcome of the reports as a [.ipynb](https://fileinfo.com/extension/ipynb) file.

#### Part 4: Infrastructure as Code & Data Pipeline with AWS CDK
0. Using [AWS CloudFormation](https://aws.amazon.com/cloudformation/), [AWS CDK](https://aws.amazon.com/cdk/) or [Terraform](https://www.terraform.io/), create a data pipeline that will automate the steps above.
1. The deployment should include a Lambda function that executes
   Part 1 and Part 2 (you can combine both in 1 lambda function). The lambda function will be scheduled to run daily.
2. The deployment should include an SQS queue that will be populated every time the JSON file is written to S3. (Hint: [S3 - Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html))
3. For every message on the queue - execute a Lambda function that outputs the reports from Part 3 (just logging the results of the queries would be enough. No .ipynb is required).

