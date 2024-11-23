from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import pika, json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@db-order-service:5432/orderdb'
db = SQLAlchemy(app)

# Define the Order model
class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    product_id = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(80), nullable=False)

# Function to process messages from RabbitMQ
def process_message(ch, method, properties, body):
    message = json.loads(body)
    print(f"Order Service received: {message}")
    user_id = message.get('user_id')
    product_id = 1  # Example product_id

    # Create a new order in the database
    with app.app_context():
        order = Order(user_id=user_id, product_id=product_id, status='order_created')
        db.session.add(order)
        db.session.commit()

    # Publish a message to the notification queue
    message['status'] = 'order_created'
    publish_message('notification_queue', message)

# Function to publish messages to RabbitMQ
def publish_message(queue, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))
    connection.close()

@app.route('/process', methods=['GET'])
def consume():
    print('rececived http request to oder service going to process')
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='order_queue')
    channel.basic_consume(queue='order_queue', on_message_callback=process_message, auto_ack=True)
    print("Order Service is consuming messages.")
    channel.start_consuming()

if __name__ == '__main__':
    # Ensure database tables are created before starting the service
    with app.app_context():
        db.create_all()

    app.run(host='0.0.0.0', port=5000)
