"""
Producer de eventos de usuario para Kafka
Genera y envía eventos de actividad de usuarios
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
from utils import load_users

# Inicializar colorama
init(autoreset=True)


class UserEventProducer:
    """Producer de eventos de usuario"""
    
    def __init__(self):
        """Inicializa el producer"""
        self.producer = None
        self.users = load_users()
        
        if not self.users:
            raise ValueError("No se pudieron cargar usuarios")
        
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
            print(f"{Fore.GREEN}✓ User Event Producer conectado a Kafka")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error conectando a Kafka: {e}")
            raise
    
    def generate_user_event(self) -> dict:
        """
        Genera un evento de usuario
        
        Returns:
            Diccionario con el evento de usuario
        """
        user = random.choice(self.users)
        event_type = random.choice(config.USER_EVENT_TYPES)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'user_id': user['user_id'],
            'user_name': user['name'],
            'user_region': user['region'],
            'user_type': user['customer_type'],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        }
        
        # Agregar información específica según el tipo de evento
        if event_type == 'search':
            event['search_query'] = random.choice([
                'laptop', 'headphones', 'mouse', 'keyboard', 
                'camera', 'speaker', 'watch', 'charger'
            ])
        elif event_type == 'profile_update':
            event['updated_field'] = random.choice([
                'email', 'address', 'phone', 'preferences'
            ])
        
        return event
    
    def send_event(self, event: dict):
        """
        Envía un evento al topic de usuarios
        
        Args:
            event: Diccionario con el evento
        """
        try:
            future = self.producer.send(config.KAFKA_TOPIC_USER_EVENTS, value=event)
            future.get(timeout=10)
            
            extra_info = ""
            if event['event_type'] == 'search':
                extra_info = f" | Query: '{event['search_query']}'"
            
            print(f"{Fore.BLUE}[{event['timestamp']}] {event['event_type'].upper():15} - "
                  f"User: {event['user_id']} | Device: {event['device']}{extra_info}")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error enviando evento: {e}")
    
    def run(self, num_events: int = None):
        """
        Ejecuta el producer generando eventos de usuario
        
        Args:
            num_events: Número de eventos a generar
        """
        num_events = num_events or config.NUM_EVENTS_PER_PRODUCER
        
        print(f"\n{Fore.YELLOW}{'='*80}")
        print(f"{Fore.YELLOW}Iniciando User Event Producer")
        print(f"{Fore.YELLOW}Generando {num_events} eventos de usuario...")
        print(f"{Fore.YELLOW}{'='*80}\n")
        
        try:
            for i in range(num_events):
                event = self.generate_user_event()
                self.send_event(event)
                
                # Delay aleatorio
                delay = random.uniform(config.PRODUCER_DELAY_MIN, config.PRODUCER_DELAY_MAX)
                time.sleep(delay)
            
            print(f"\n{Fore.GREEN}✓ User Event Producer completado: {num_events} eventos enviados\n")
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
    producer = UserEventProducer()
    producer.run()