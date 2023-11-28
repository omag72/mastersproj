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
   cd 

License
This project is licensed under the MIT License - see the LICENSE file for details



























