"""
Consumer que escucha los 3 topics simultáneamente
Muestra todos los eventos en tiempo real
"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from colorama import Fore, Style, init
import config

# Inicializar colorama
init(autoreset=True)


class AllTopicsConsumer:
    """Consumer que lee de todos los topics"""
    
    def __init__(self):
        """Inicializa el consumer para múltiples topics"""
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """Establece conexión con Kafka para los 3 topics"""
        try:
            self.consumer = KafkaConsumer(
                config.KAFKA_TOPIC_PRODUCT_EVENTS,
                config.KAFKA_TOPIC_PURCHASES,
                config.KAFKA_TOPIC_USER_EVENTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset=config.CONSUMER_AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                group_id=f"{config.CONSUMER_GROUP_ID}-all",
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"{Fore.GREEN}✓ Consumer conectado a los 3 topics:")
            print(f"  • {config.KAFKA_TOPIC_PRODUCT_EVENTS}")
            print(f"  • {config.KAFKA_TOPIC_PURCHASES}")
            print(f"  • {config.KAFKA_TOPIC_USER_EVENTS}\n")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error conectando consumer: {e}")
            raise
    
    def process_event(self, topic: str, event: dict):
        """
        Procesa un evento de cualquier topic
        
        Args:
            topic: Nombre del topic
            event: Diccionario con el evento
        """
        event_type = event.get('event_type', 'unknown')
        timestamp = event.get('timestamp', 'N/A')
        
        # Colores por topic
        if topic == config.KAFKA_TOPIC_PRODUCT_EVENTS:
            color = Fore.CYAN
            topic_label = "[PRODUCT]"
            details = f"User: {event.get('user_id')} | Product: {event.get('product_name')}"
        
        elif topic == config.KAFKA_TOPIC_PURCHASES:
            color = Fore.MAGENTA
            topic_label = "[PURCHASE]"
            details = (f"User: {event.get('user_id')} | Product: {event.get('product_name')} | "
                      f"Qty: {event.get('quantity')} | Total: ${event.get('total_amount')}")
        
        elif topic == config.KAFKA_TOPIC_USER_EVENTS:
            color = Fore.BLUE
            topic_label = "[USER]"
            details = f"User: {event.get('user_id')} | Device: {event.get('device')}"
            if event_type == 'search':
                details += f" | Query: '{event.get('search_query')}'"
        
        else:
            color = Fore.WHITE
            topic_label = "[UNKNOWN]"
            details = str(event)
        
        print(f"{color}{topic_label:12} [{timestamp}] {event_type.upper():18} | {details}")
    
    def run(self):
        """Inicia el consumer en modo escucha continua"""
        print(f"{Fore.YELLOW}{'='*80}")
        print(f"{Fore.YELLOW}Esperando eventos de todos los topics...")
        print(f"{Fore.YELLOW}{'='*80}\n")
        
        try:
            for message in self.consumer:
                self.process_event(message.topic, message.value)
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠ Consumer detenido por usuario\n")
        finally:
            self.close()
    
    def close(self):
        """Cierra la conexión"""
        if self.consumer:
            self.consumer.close()
            print(f"{Fore.GREEN}✓ Consumer cerrado")


if __name__ == '__main__':
    consumer = AllTopicsConsumer()
    consumer.run()