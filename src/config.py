"""
Configuración del proyecto Kafka
Carga variables de entorno y define constantes del sistema
"""
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC_PRODUCT_EVENTS = os.getenv('KAFKA_TOPIC_PRODUCT_EVENTS', 'product-events')
KAFKA_TOPIC_USER_EVENTS = os.getenv('KAFKA_TOPIC_USER_EVENTS', 'user-events')
KAFKA_TOPIC_PURCHASES = os.getenv('KAFKA_TOPIC_PURCHASES', 'purchases')

# Configuración del Producer
PRODUCER_DELAY_MIN = int(os.getenv('PRODUCER_DELAY_MIN', 1))
PRODUCER_DELAY_MAX = int(os.getenv('PRODUCER_DELAY_MAX', 5))
NUM_EVENTS_PER_PRODUCER = int(os.getenv('NUM_EVENTS_PER_PRODUCER', 50))

# Configuración del Consumer
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'analytics-group')
CONSUMER_AUTO_OFFSET_RESET = os.getenv('CONSUMER_AUTO_OFFSET_RESET', 'earliest')

# Rutas de archivos de datos
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
PRODUCTS_FILE = os.path.join(DATA_DIR, 'products.json')
USERS_FILE = os.path.join(DATA_DIR, 'users.json')

# Tipos de eventos por topic
PRODUCT_EVENT_TYPES = ['view', 'add_to_cart', 'remove_from_cart']
USER_EVENT_TYPES = ['login', 'logout', 'profile_update', 'search']
PURCHASE_EVENT_TYPES = ['purchase']