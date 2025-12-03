"""
Producer de eventos de productos para Kafka
Genera y envía eventos de interacción con productos (NO compras)
"""
import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from colorama import Fore, Style, init
import config
from utils import load_products, load_users, create_event, format_event_for_display

# Inicializar colorama
init(autoreset=True)


class ProductEventProducer:
    """Producer de eventos de productos (view, add_to_cart, remove_from_cart)"""
    
    def __init__(self, product_id: str = None):
        """
        Inicializa el producer
        
        Args:
            product_id: ID del producto específico (opcional)
        """
        self.product_id = product_id
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
            print(f"{Fore.GREEN}✓ Product Event Producer conectado a Kafka")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error conectando a Kafka: {e}")
            raise
    
    def _get_product(self):
        """Obtiene un producto (específico o aleatorio)"""
        if self.product_id:
            product = next((p for p in self.products if p['product_id'] == self.product_id), None)
            if not product:
                raise ValueError(f"Producto {self.product_id} no encontrado")
            return product
        return random.choice(self.products)
    
    def generate_event(self) -> dict:
        """
        Genera un evento aleatorio de producto (sin compras)
        
        Returns:
            Diccionario con el evento generado
        """
        # Solo eventos de producto (NO compras)
        event_type = random.choice(config.PRODUCT_EVENT_TYPES)
        user = random.choice(self.users)
        product = self._get_product()
        
        return create_event(event_type, user, product)
    
    def send_event(self, event: dict):
        """
        Envía un evento al topic de productos
        
        Args:
            event: Diccionario con el evento
        """
        try:
            future = self.producer.send(config.KAFKA_TOPIC_PRODUCT_EVENTS, value=event)
            future.get(timeout=10)
            print(f"{Fore.CYAN}{format_event_for_display(event)}")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error enviando evento: {e}")
    
    def run(self, num_events: int = None):
        """
        Ejecuta el producer generando eventos
        
        Args:
            num_events: Número de eventos a generar (None = infinito)
        """
        num_events = num_events or config.NUM_EVENTS_PER_PRODUCER
        product_name = self._get_product()['name'] if self.product_id else "Todos los productos"
        
        print(f"\n{Fore.YELLOW}{'='*80}")
        print(f"{Fore.YELLOW}Iniciando Product Event Producer: {product_name}")
        print(f"{Fore.YELLOW}Generando {num_events} eventos...")
        print(f"{Fore.YELLOW}{'='*80}\n")
        
        try:
            for i in range(num_events):
                event = self.generate_event()
                self.send_event(event)
                
                # Delay aleatorio entre eventos
                delay = random.uniform(config.PRODUCER_DELAY_MIN, config.PRODUCER_DELAY_MAX)
                time.sleep(delay)
            
            print(f"\n{Fore.GREEN}✓ Producer completado: {num_events} eventos enviados\n")
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
    import sys
    
    # Permitir especificar un product_id como argumento
    product_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    producer = ProductEventProducer(product_id=product_id)
    producer.run()