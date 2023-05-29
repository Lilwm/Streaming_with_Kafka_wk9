!pip install confluent-kafka

#import necessary libraries
from confluent_kafka import Producer, Consumer
import json
import pandas as pd
import matplotlib.pyplot as plt

#Set up kafka producer config
producer = Producer({
    'bootstrap.servers':'pkc-q283m.af-south-1.aws.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':'WVXAVKT5ZOZYZ2ST',
    'sasl.password':'PIfNcb/jzZoBQbIYnuyduoXDymELg86Nuoj6jsyGlwqzSO7Q5Xqc2R/z+4obL5l/'
})

#define message payload
txn_message = {
"transaction_id": "12345",
"sender_phone_number": "256777123456",
"receiver_phone_number": "256772987654",
"transaction_amount": 100000,
"transaction_time": "2023-04-19 12:00:00"
}

# write to mobile money topic
producer.produce('mobile_money', value =json.dumps(txn_message) )
producer.flush()

#set up consumer config
consumer = Consumer({
    'bootstrap.servers':'pkc-q283m.af-south-1.aws.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':'WVXAVKT5ZOZYZ2ST',
    'sasl.password':'PIfNcb/jzZoBQbIYnuyduoXDymELg86Nuoj6jsyGlwqzSO7Q5Xqc2R/z+4obL5l/',
    'group.id': 'Assignment',
    'auto.offset.reset': 'earliest'
    
})

#subscribe to the kafka topic
consumer.subscribe(['mobile_money'])

#transform data for analysis
def calculate_transaction_value(df):
  # Group the DataFrame by day of week and hour of day.
  grouped = df.groupby(['day_of_week', 'hour_of_day'])

  # Calculate the sum of the transaction amount for each group.
  transaction_value = grouped['transaction_amount'].sum()

  # Return the DataFrame with the transaction value by day of week and hour of day.
  return transaction_value

# Start consuming messages
while True:
    # Get the next message
    message = consumer.poll(0.1)

    # If there is a message
    if message:
        # Convert the message to a DataFrame.
        df = pd.DataFrame([message.value])

        # Convert the transaction_time column to a datetime object.
        df['transaction_time'] = pd.to_datetime(df['transaction_time'])

        # Add a column for the day of the week and hour.
        df['day_of_week'] = df['transaction_time'].dt.dayofweek
        df['hour_of_day'] = df['transaction_time'].dt.hour

        # Calculate the transaction value by day of week and hour of day.
        transaction_value = calculate_transaction_value(df)

        # Plot the DataFrame as a bar chart.
        plt.bar(transaction_value['day_of_week'], transaction_value['transaction_value'])

        # Set the title of the plot.
        plt.title('Transaction Value by Day of Week and Hour of Day')

        # Set the x-axis label.
        plt.xlabel('Day of Week')

        # Set the y-axis label.
        plt.ylabel('Transaction Value')

        # Show the plot.
        plt.show()

