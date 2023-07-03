import spark
import kafka
import json

# Create a Spark session
spark = spark.sql.SparkSession.builder.appName("Fraud Detection").getOrCreate()

# Create a Kafka consumer
consumer = kafka.KafkaConsumer("fraud-detection", bootstrap_servers="localhost:9092")

# Process the data from Kafka
for message in consumer:
    # Get the data from the message
    data = json.loads(message.value)

    # Check if the transaction is fraudulent
    if is_fraudulent(data):
        # Save the result to the database
        db.insert_row(data)

# Close the consumer
consumer.close()

def is_fraudulent(transaction):
    # Check if the transaction amount is too high
    if transaction["amount"] > 10000:
        return True

    # Check if the transaction was made from a different country
    if transaction["country"] != "US":
        return True

    # Check if the transaction was made with a credit card that has been flagged for fraud
    if transaction["credit_card_number"] in flagged_credit_cards:
        return True

    # Return False if the transaction is not fraudulent
    return False

def db.insert_row(transaction):
    # Connect to the database
    conn = pymysql.connect(host="localhost", user="root", password="password", database="fraud_detection")

    # Create a cursor
    cursor = conn.cursor()

    # Insert the row into the database
    cursor.execute("INSERT INTO transactions (amount, country, credit_card_number, is_fraudulent) VALUES (%s, %s, %s, %s)", (transaction["amount"], transaction["country"], transaction["credit_card_number"], transaction["is_fraudulent"]))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cursor.close()

    # Close the connection to the database
    conn.close()