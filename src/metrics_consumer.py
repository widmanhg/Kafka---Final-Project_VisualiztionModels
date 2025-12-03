"""
Consumer de métricas y análisis
Procesa eventos y genera estadísticas en tiempo real
"""
import json
from collections import defaultdict, Counter
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from colorama import Fore, Style, init
import config

# Inicializar colorama
init(autoreset=True)


class MetricsConsumer:
    """Consumer que calcula métricas y análisis de eventos"""
    
    def __init__(self):
        """Inicializa el consumer de métricas"""
        self.consumer = None
        self.metrics = {
            'total_events': 0,
            'events_by_type': Counter(),
            'events_by_region': Counter(),
            'events_by_customer_type': Counter(),
            'product_views': Counter(),
            'products_added_to_cart': Counter(),
            'purchases_by_product': Counter(),
        }
        self._connect()
    
    def _connect(self):
        """Establece conexión con Kafka"""
        try:
            self.consumer = KafkaConsumer(
                config.KAFKA_TOPIC_PRODUCT_EVENTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset=config.CONSUMER_AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                group_id=f"{config.CONSUMER_GROUP_ID}-metrics",
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"{Fore.GREEN}✓ Metrics Consumer conectado")
            print(f"{Fore.GREEN}✓ Topic: {config.KAFKA_TOPIC_PRODUCT_EVENTS}\n")
        except KafkaError as e:
            print(f"{Fore.RED}✗ Error conectando consumer: {e}")
            raise
    
    def process_event(self, event: dict):
        """
        Procesa un evento y actualiza métricas
        
        Args:
            event: Diccionario con el evento
        """
        self.metrics['total_events'] += 1
        
        event_type = event.get('event_type')
        user_region = event.get('user_region')
        user_type = event.get('user_type')
        product_name = event.get('product_name')
        
        # Actualizar contadores
        if event_type:
            self.metrics['events_by_type'][event_type] += 1
        
        if user_region:
            self.metrics['events_by_region'][user_region] += 1
        
        if user_type:
            self.metrics['events_by_customer_type'][user_type] += 1
        
        # Métricas específicas por tipo de evento
        if event_type == 'view' and product_name:
            self.metrics['product_views'][product_name] += 1
        elif event_type == 'add_to_cart' and product_name:
            self.metrics['products_added_to_cart'][product_name] += 1
        elif event_type == 'purchase' and product_name:
            self.metrics['purchases_by_product'][product_name] += 1
    
    def display_metrics(self):
        """Muestra las métricas actuales en consola"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}MÉTRICAS EN TIEMPO REAL")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        print(f"{Fore.YELLOW}Total de eventos procesados: {Fore.WHITE}{self.metrics['total_events']}\n")
        
        # Eventos por tipo
        print(f"{Fore.GREEN}Eventos por tipo:")
        for event_type, count in self.metrics['events_by_type'].most_common():
            print(f"  • {event_type:20} : {count}")
        
        # Eventos por región
        print(f"\n{Fore.GREEN}Eventos por región:")
        for region, count in self.metrics['events_by_region'].most_common():
            print(f"  • {region:20} : {count}")
        
        # Eventos por tipo de cliente
        print(f"\n{Fore.GREEN}Eventos por tipo de cliente:")
        for customer_type, count in self.metrics['events_by_customer_type'].most_common():
            print(f"  • {customer_type:20} : {count}")
        
        # Top 5 productos más vistos
        print(f"\n{Fore.MAGENTA}Top 5 productos más vistos:")
        for product, count in self.metrics['product_views'].most_common(5):
            print(f"  • {product:35} : {count}")
        
        # Top 5 productos más agregados al carrito
        print(f"\n{Fore.MAGENTA}Top 5 productos agregados al carrito:")
        for product, count in self.metrics['products_added_to_cart'].most_common(5):
            print(f"  • {product:35} : {count}")
        
        # Top 5 productos más comprados
        if self.metrics['purchases_by_product']:
            print(f"\n{Fore.MAGENTA}Top 5 productos más comprados:")
            for product, count in self.metrics['purchases_by_product'].most_common(5):
                print(f"  • {product:35} : {count}")
        
        print(f"\n{Fore.CYAN}{'='*80}\n")
    
    def run(self, display_interval: int = 20):
        """
        Inicia el consumer y muestra métricas periódicamente
        
        Args:
            display_interval: Número de eventos entre cada visualización de métricas
        """
        print(f"{Fore.YELLOW}{'='*80}")
        print(f"{Fore.YELLOW}Iniciando análisis de métricas...")
        print(f"{Fore.YELLOW}Mostrando resultados cada {display_interval} eventos")
        print(f"{Fore.YELLOW}{'='*80}\n")
        
        try:
            for message in self.consumer:
                self.process_event(message.value)
                
                # Mostrar métricas cada X eventos
                if self.metrics['total_events'] % display_interval == 0:
                    self.display_metrics()
        
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠ Análisis detenido por usuario")
            self.display_metrics()
        finally:
            self.close()
    
    def close(self):
        """Cierra la conexión"""
        if self.consumer:
            self.consumer.close()
            print(f"\n{Fore.GREEN}✓ Metrics Consumer cerrado")


if __name__ == '__main__':
    consumer = MetricsConsumer()
    consumer.run()