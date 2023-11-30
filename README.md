# mastersproj - Realtime kafka spark cassandra mysql machine learning and bokeh 
The project is a real time kafka spak cassandra mysql and bokeh project.
The project generates fake data and pushes it to a kafka producer which then send s the data to a spark application for processing 
and the data is then stored in cassandra database.The spark application further aggregates the data stored in cassandra, 
adds another column (total_sales)and stores the aggregated data in mysql  database.
The data in mysql is then consumed by a machine learning algorithm which utilises the data to predict monthly demand ,
calculates daily sales and visualises them on bokeh and matplotlib. 

## Technologies Used

- Apache Kafka
- Apache Spark
- Apache Cassandra
- MySQL
- Bokeh
- Docker

## Getting Started

### Prerequisites

Ensure you have the following software installed on your machine:

- Docker: [Get Docker](https://docs.docker.com/get-docker/)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/omag72/mastersproj.git

Installation Instructions 
Install important libraries 

pip install confluent_kafka , 
pip install faker, 
pip install pyspark,
pip install cassandra-driver,
pip install mysql-connector-java==8.0.26

Running the project
PS C:\Users\ceo\mastersproj> 
enter into the python environment using:  env/Scripts/Activate.ps1
 
Open a new tab and run the producer.py
 

## pyspark container:  cassandra_consumer.py
(env) PS C:\Users\ceo\mastersproj\lab>
Enter:  docker exec -it pyspark-container /bin/bash
(env) PS C:\Users\ceo\mastersproj\lab> docker exec -it pyspark-container /bin/bash
(base) jovyan@bd8ef966c0dc:~$ 
Enter:  spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
--jars /home/jovyan/work/jars/mysql-connector-java-8.0.26.jar,/home/jovyan/work/jars/cassandra-driver-core-3.11.3.jar,/home/jovyan/work/jars/mysql-connector-java-8.0.26.jar \
--conf spark.cassandra.connection.host=cassandra \
/home/jovyan/work/cassandra_consumer.py
 

## pyspark container: mysql_consumer.py
(env) PS C:\Users\ceo\mastersproj\lab>
Enter: docker exec -it pyspark-container /bin/bash
(env) PS C:\Users\ceo\mastersproj\lab> docker exec -it pyspark-container /bin/bash
(base) jovyan@bd8ef966c0dc:~$ 
Enter: spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
--jars /home/jovyan/work/jars/mysql-connector-java-8.0.26.jar,/home/jovyan/work/jars/cassandra-driver-core-3.11.3.jar,/home/jovyan/work/jars/mysql-connector-java-8.0.26.jar \
--conf spark.cassandra.connection.host=cassandra \
/home/jovyan/work/mysql_consumer.py
 
## cassandra container:
(env) PS C:\Users\ceo\mastersproj\lab> 
Enter: docker exec -it cassandra cqlsh -u cassandra -p cassandra
cassandra@cqlsh> 
Enter: describe keyspaces;
cassandra@cqlsh> 
Enter: use car_parts;
cassandra@cqlsh> use car_parts;
Enter: describe tables;
cassandra@cqlsh:car_parts> 
Enter: select * from sales_data;
  
## mysql container:
(env) PS C:\Users\ceo\mastersproj\lab>
Enter  docker exec -it mysql bash
(env) PS C:\Users\ceo\mastersproj\lab> docker exec -it mysql bash
bash-4.4# 
Enter: mysql -u myuser -p
bash-4.4# mysql -u myuser -p
Enter password: mypassword
mysql> 
Enter: show databases;
mysql> Enter: use mydatabase;
mysql> Enter show tables;
Enter select command 
mysql> select * from sales_data;
 
 
## notebook: visuals.py 
Click on pyspark container port : 8888:8888
click on visuals.ipynb
 

## Project github link: https://github.com/omag72/mastersproj

## License
This project is licensed under the MIT License - see the LICENSE file for details



























