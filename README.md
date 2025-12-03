# Kafka Project - Real-Time Streaming System

Real-time data streaming system using Apache Kafka to simulate and analyze user behavior events on an e-commerce platform.

## 📋 Description

This project implements a distributed event architecture that simulates user interactions with products (views, add to cart, remove from cart, purchases) and processes these events in real-time to generate metrics and behavior analysis.

## 🏗️ Architecture

- **Zookeeper**: Kafka service coordination
- **Kafka**: Distributed messaging system with 3 topics:
  - `product-events`: Product interactions (view, add_to_cart, remove_from_cart)
  - `purchases`: Completed purchases with payment details
  - `user-events`: User activity (login, logout, search, profile_update)
- **Kafka UI**: Web interface for monitoring
- **Producers**: 3 types of event generators
- **Consumers**: Event processors and metrics generators

## 📁 Project Structure

```
kafka-project/
├── docker-compose.yml          # Container configuration
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── data/
│   ├── products.json          # Product catalog
│   └── users.json             # User database
├── src/
│   ├── __init__.py
│   ├── config.py              # Project configuration
│   ├── producer.py            # Product events producer
│   ├── purchase_producer.py   # Purchase producer
│   ├── user_producer.py       # User events producer
│   ├── consumer.py            # All topics consumer
│   ├── metrics_consumer.py    # Metrics consumer
│   └── utils.py               # Helper functions
└── scripts/
    ├── run_multiple_producers.py  # Multiple producer executor
    └── setup_topics.py            # Topic configurator
```

## 🚀 Installation and Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Conda or venv

### Step 1: Clone the Repository

```bash
git clone <your-repository>
cd kafka-project
```

### Step 2: Set Up Virtual Environment

**Using Conda:**
```bash
conda create -n kafka-env python=3.10
conda activate kafka-env
```

**Using venv:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start Kafka Infrastructure

```bash
docker-compose up -d
```

This will start:
- Zookeeper on port 2181
- Kafka on port 9092
- Kafka UI at http://localhost:8080

### Step 5: Create Topics

```bash
python scripts/setup_topics.py
```

## 🎯 System Usage

### Verify Kafka UI

Open your browser at: http://localhost:8080

You should see the created topics:
- `product-events`
- `user-events`
- `purchases`

### Run an Individual Producer

**Product Producer:**
```bash
cd src
python producer.py
```

**Purchase Producer:**
```bash
cd src
python purchase_producer.py
```

**User Events Producer:**
```bash
cd src
python user_producer.py
```

### Run Multiple Producers Simultaneously

```bash
python scripts/run_multiple_producers.py
```

This will run **3 concurrent producers**, one for each topic:
- **Product Events**: 40 product interaction events
- **Purchases**: 15 completed purchase events
- **User Events**: 30 user activity events

### View Events from All Topics

In a new terminal:
```bash
cd src
python consumer.py
```

This consumer will display events from all 3 topics simultaneously with different colors.



You'll see events in real-time as they arrive.

### Run the Metrics Consumer

In a new terminal:
```bash
cd src
python metrics_consumer.py
```

This consumer will display analysis and statistics every 20 events:
- Total events by type
- Events by region
- Events by customer type
- Top 5 most viewed products
- Top 5 products added to cart
- Top 5 most purchased products

## 📊 Event Types

### Topic: product-events
1. **view**: User views a product
2. **add_to_cart**: User adds product to cart
3. **remove_from_cart**: User removes product from cart

### Topic: purchases
1. **purchase**: User completes purchase (includes quantity, total, payment method)

### Topic: user-events
1. **login**: User logs in
2. **logout**: User logs out
3. **search**: User searches for products
4. **profile_update**: User updates their profile

## 📈 Calculated Metrics

- Event count by type
- Segmentation by region (North, South, East, West)
- Segmentation by customer type (Regular, Premium, VIP)
- Most popular products
- View-to-purchase conversion

## 🛠️ Advanced Configuration

### Modify Environment Variables

Edit the `.env` file to adjust:
- Kafka servers
- Topic names
- Delays between events
- Number of events per producer

### Add More Products

Edit `data/products.json` and add new products with the structure:

```json
{
  "product_id": "P011",
  "name": "New Product",
  "category": "Category",
  "price": 99.99,
  "brand": "Brand",
  "stock": 100
}
```

### Add More Users

Edit `data/users.json` and add new users with the structure:

```json
{
  "user_id": "U009",
  "name": "User Name",
  "email": "email@example.com",
  "region": "North",
  "customer_type": "Premium",
  "age": 30,
  "interests": ["Electronics"]
}
```

## 🧪 Test Use Cases

### Case 1: Simulate High Traffic

```bash
# Terminal 1: Run multiple producers
python scripts/run_multiple_producers.py

# Terminal 2: Monitor with metrics consumer
cd src && python metrics_consumer.py
```


### Case 3: Kafka UI Monitoring

1. Open http://localhost:8080
2. Go to Topics → product-events
3. Click on "Messages" to see events in real-time
4. Observe partitions and offsets

## 🔍 Troubleshooting

### Kafka Won't Connect

```bash
# Verify containers are running
docker-compose ps

# View logs
docker-compose logs kafka
```

### Topics Not Created

```bash
# Recreate topics
python scripts/setup_topics.py
```

### Import Errors

```bash
# Make sure you're in the virtual environment
conda activate kafka-env  # or source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

