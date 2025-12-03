"""
Producer de eventos de compras para Kafka
Genera y envía eventos de compras completadas
"""
import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from colorama import Fore, Style, init
import config
from utils import load_products, load_users

# Inicializar colorama
init(autoreset=True)


class PurchaseProducer:
    """Producer de eventos de compras"""
    
    def __init__(self):
        """Inicializa el producer"""
        self.producer = None
        self.products = load_products()
        self.users = load_users()
        
        if not self.products or not self.users:
            raise ValueError("No se pudieron cargar productos o usuarios")
        
        self._connect()
    
    def _connect(self):
        """Establece conexión con Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"{Fore.GREEN}✓ Purchase Producer conectado a Kafka")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error conectando a Kafka: {e}")
            raise
    
    def generate_purchase_event(self) -> dict:
        """
        Genera un evento de compra
        
        Returns:
            Diccionario con el evento de compra
        """
        user = random.choice(self.users)
        product = random.choice(self.products)
        quantity = random.randint(1, 3)
        
        return {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'event_type': 'purchase',
            'user_id': user['user_id'],
            'user_name': user['name'],
            'user_region': user['region'],
            'user_type': user['customer_type'],
            'product_id': product['product_id'],
            'product_name': product['name'],
            'product_category': product['category'],
            'product_price': product['price'],
            'quantity': quantity,
            'total_amount': round(product['price'] * quantity, 2),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'cash'])
        }
    
    def send_event(self, event: dict):
        """
        Envía un evento al topic de compras
        
        Args:
            event: Diccionario con el evento
        """
        try:
            future = self.producer.send(config.KAFKA_TOPIC_PURCHASES, value=event)
            future.get(timeout=10)
            print(f"{Fore.MAGENTA}[{event['timestamp']}] PURCHASE - "
                  f"User: {event['user_id']} | Product: {event['product_name']} | "
                  f"Qty: {event['quantity']} | Total: ${event['total_amount']}")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error enviando evento: {e}")
    
    def run(self, num_events: int = None):
        """
        Ejecuta el producer generando eventos de compra
        
        Args:
            num_events: Número de eventos a generar
        """
        num_events = num_events or config.NUM_EVENTS_PER_PRODUCER
        
        print(f"\n{Fore.YELLOW}{'='*80}")
        print(f"{Fore.YELLOW}Iniciando Purchase Producer")
        print(f"{Fore.YELLOW}Generando {num_events} eventos de compra...")
        print(f"{Fore.YELLOW}{'='*80}\n")
        
        try:
            for i in range(num_events):
                event = self.generate_purchase_event()
                self.send_event(event)
                
                # Delay aleatorio (compras son menos frecuentes)
                delay = random.uniform(config.PRODUCER_DELAY_MIN * 2, config.PRODUCER_DELAY_MAX * 2)
                time.sleep(delay)
            
            print(f"\n{Fore.GREEN}✓ Purchase Producer completado: {num_events} eventos enviados\n")
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠ Producer interrumpido por usuario\n")
        finally:
            self.close()
    
    def close(self):
        """Cierra la conexión con Kafka"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print(f"{Fore.GREEN}✓ Conexión cerrada")


if __name__ == '__main__':
    producer = PurchaseProducer()
    producer.run()