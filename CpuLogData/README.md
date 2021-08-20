Program Aim:
Program to create a dataframe from cpu log data.csv file using pyspark library,and perform different operations and also do visualization of the result.
Columns to consider user_name,DateTime,keyboard,mouse
Display users and their record counts
Finding users with highest number of average hours
Finding users with lowest number of average hours
Finding users with highest numbers of idle hours
Finding users with highest numbers of times late comings
Step1:

    Open terminal and start hadoop daemons:
    start-all-sh
    Check for spark daemon running or not by using jps command in terminal
    Load all csv into hdfs using put command into a folder.
    Open vscode and create a .ipynb(notebook file) and name your file name.

Step2:
Import the necessary library and create a spark session:

    from pyspark.sql import *
    from pyspark.sql import functions as func
    spark = SparkSession.builder.getOrCreate()

Creating a dataframe by loading a csv file from hdfs:

    df = spark.read.csv("hdfs://localhost:9000/Spark_Sql_Task/*.csv",header=True)

Considering only these 4 columns from that csv file thus creating new dataframe:

    df2 = df.select("user_name","DateTime","keyboard","mouse")

Step3: Now completing first task:
Display users and their record counts:
Here from df2 we have to group our data with username and get count and saving it to df10.

    df10 = df2.groupBy("user_name").count()
    df10.show()

Output:

    +--------------------+-----+
    | user_name|count|
    +--------------------+-----+
    |salinabodale73@gm...| 569|
    |sharlawar77@gmail...| 580|
    |rahilstar11@gmail...| 551|
    |deepshukla292@gma...| 565|
    | iamnzm@outlook.com| 614|
    |markfernandes66@g...| 508|
    |damodharn21@gmail...| 253|
    |bhagyashrichalke2...| 482|
    +--------------------+-----+

Step4:
Finding users with highest number of average hours:
For these tasks we will create a tempview named view1 from the original df.
Next we will be running a sql query and getting only those data where the user is active and finally grouping it with username and getting count and saving it to df1.

    df.createOrReplaceTempView("view1")

    df1 = spark.sql("select user_name from view1 where keyboard != 0 or mouse != 0").groupBy("user_name").count()

    df1.show(truncate=False)

    +----------------------------+-----+

    |user_name |count|

    +----------------------------+-----+

    |salinabodale73@gmail.com |436 |

    |sharlawar77@gmail.com |457 |

    |rahilstar11@gmail.com |399 |

    |deepshukla292@gmail.com |475 |

    |iamnzm@outlook.com |459 |

    |markfernandes66@gmail.com |389 |

    |damodharn21@gmail.com |191 |

    |bhagyashrichalke21@gmail.com|361 |

    +----------------------------+-----+

Now Again we will be creating a view as hour_view of these df1 and save it to df3.

    df3 = df1.createOrReplaceTempView("hour_view")

Now :

    We know that the diff of every record is with difference of 5 min we multiply by 5
    We need the average time in Hours therefore we multiply by 60
    There are 6 days of data available, therefore we divide them by 6
    Here is the query for that:
    df4 = spark.sql("select user_name,((((count-1)*5)*60)/6) as hours from hour_view")
    Here,we are running sql query on the previously created view hour_view and saving the results which is time in seconds in a column hours and finally saving it to df4.
    Here,count-1 is used to skip the first row.
    df4.show(truncate=False)
    +----------------------------+-------+
    |user_name |hours |
    +----------------------------+-------+
    |salinabodale73@gmail.com |21750.0|
    |sharlawar77@gmail.com |22800.0|
    |rahilstar11@gmail.com |19900.0|
    |deepshukla292@gmail.com |23700.0|
    |iamnzm@outlook.com |22900.0|
    |markfernandes66@gmail.com |19400.0|
    |damodharn21@gmail.com |9500.0 |
    |bhagyashrichalke21@gmail.com|18000.0|
    +----------------------------+-------+

Now convert these hour column into hh:mm format,
And printing the final result:

    from pyspark.sql.functions import *
    highest_avg_hour = df4.withColumn("average_hours", concat(
    floor(col("hours") % 86400 / 3600), lit(":"),
    floor((col("hours") % 86400) % 3600 / 60), lit(""),))\
    .drop("hours")

Output:highest_avg_hour.show():

    +--------------------+-------------+
    | user_name|average_hours|
    +--------------------+-------------+
    |salinabodale73@gm...| 6:2|
    |sharlawar77@gmail...| 6:20|
    |rahilstar11@gmail...| 5:31|
    |deepshukla292@gma...| 6:35|
    | iamnzm@outlook.com| 6:21|
    |markfernandes66@g...| 5:23|
    |damodharn21@gmail...| 2:38|
    |bhagyashrichalke2...| 5:0|
    +--------------------+-------------+

Step5:
Finding users with lowest number of average hours:
For getting lowest average hours we just have to sort our previous output into ascending order:

    from pyspark.sql.functions import *
    lowest_avg_hour = df4.withColumn("average_hours", concat(
    floor(col("hours") % 86400 / 3600), lit(":"),
    floor((col("hours") % 86400) % 3600 / 60), lit(""),))\
    .drop("hours")
    .sort(asc("average_hours"))

Output: lowest_avg_hour.show():

    +--------------------+-------------+
    | user_name|average_hours|
    +--------------------+-------------+
    |damodharn21@gmail...| 2:38|
    |bhagyashrichalke2...| 5:0|
    |markfernandes66@g...| 5:23|
    |rahilstar11@gmail...| 5:31|
    |salinabodale73@gm...| 6:2|
    |sharlawar77@gmail...| 6:20|
    | iamnzm@outlook.com| 6:21|
    |deepshukla292@gma...| 6:35|
    +--------------------+-------------+

Step6:

    Finding users with highest numbers of idle hours:

    Now finding user with highest number of idle hours we will be creating ad df5 with username and count where user is not active and checking if keyboard and mouse value is zero then user is inactive by running a sql query in our previously created view1:

    df5 = spark.sql("select user_name from view1 where keyboard == 0 and mouse == 0").groupBy("user_name").count()

    df5.show(truncate=False)

    +----------------------------+-----+

    |user_name |count|

    +----------------------------+-----+

    |salinabodale73@gmail.com |133 |

    |sharlawar77@gmail.com |123 |

    |rahilstar11@gmail.com |152 |

    |deepshukla292@gmail.com |90 |

    |iamnzm@outlook.com |155 |

    |markfernandes66@gmail.com |119 |

    |damodharn21@gmail.com |62 |

    |bhagyashrichalke21@gmail.com|121 |

    +----------------------------+-----+

Now Again we will be creating a view as idle_hour_view of these df5.

    df5.createOrReplaceTempView("idle_hour_view")

Now :

    We know that the diff of every record is with difference of 5 min we multiply by 5

    We need the average time in Hours therefore we multiply by 60

    There are 6 days of data available, therefore we divide them by 6

    Here is the query for that:

    df6 = spark.sql("select user_name,((((count-1)*5)*60)/6) as hours from hour_view")

    Here,we are running sql query on the previously created view hour_view and saving the results which is time in seconds in a column hours and finally saving it to df6.

    Here,count-1 is used to skip the first row.

    df6.show(truncate=False)

    +----------------------------+-----+-----------+

    |user_name |count|average_min|

    +----------------------------+-----+-----------+

    |salinabodale73@gmail.com |133 |6600.0 |

    |sharlawar77@gmail.com |123 |6100.0 |

    |rahilstar11@gmail.com |152 |7550.0 |

    |deepshukla292@gmail.com |90 |4450.0 |

    |iamnzm@outlook.com |155 |7700.0 |

    |markfernandes66@gmail.com |119 |5900.0 |

    |damodharn21@gmail.com |62 |3050.0 |

    |bhagyashrichalke21@gmail.com|121 |6000.0 |

    +----------------------------+-----+-----------+

Now convert these hour column into hh:mm format,
And printing the final result:

    from pyspark.sql.functions import *
    idle_hour = df6.withColumn("idle_hours", concat(
    floor(col("average_min") % 86400 / 3600), lit(":"),
    floor((col("average_min") % 86400) % 3600 / 60), lit(""),))\
    .drop("average_min")\
    .sort(desc("idle_hours"))
    Now showing the output:
    idle_hour.show(truncate=False)

Output:

    +----------------------------+-----+----------+
    |user_name |count|idle_hours|
    +----------------------------+-----+----------+
    |iamnzm@outlook.com |155 |2:8 |
    |rahilstar11@gmail.com |152 |2:5 |
    |salinabodale73@gmail.com |133 |1:50 |
    |sharlawar77@gmail.com |123 |1:41 |
    |bhagyashrichalke21@gmail.com|121 |1:40 |
    |markfernandes66@gmail.com |119 |1:38 |
    |deepshukla292@gmail.com |90 |1:14 |
    |damodharn21@gmail.com |62 |0:50 |
    +----------------------------+-----+----------+
