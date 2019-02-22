sudo /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

sudo /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
--replication-factor 1 --partitions 1 --topic order_count


Creating a topic first 

sudo /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
--replication-factor 1 --partitions 1 --topic order_count

#Now I will be running the producer and consumer code together to show the code running and the processing 

#Producer code: 
python orders_producer.py localhost:9092 order_count
#Consumer code: 
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar orders_update_state_by_key.py localhost:9092 order_count



####Starting Apache Superset #####
virtualenv -p python3.6 new_envs
cd new_env/
source bin/activate



# Create an admin user (you will be prompted to set a username, first and last name before setting a password)
fabmanager create-admin --app superset

# Initialize the database
superset db upgrade

# Load some data to play with
superset load_examples

# Create default roles and permissions
superset init

# To start a development web server on port 8088, use -p to bind to another port
superset runserver -d
