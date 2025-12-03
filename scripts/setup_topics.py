"""
Script para crear y configurar los topics de Kafka
"""
import sys
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from colorama import Fore, init

# Agregar el directorio src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import config

init(autoreset=True)


def create_topics():
    """Crea los topics necesarios para el proyecto"""
    topics = [
        {
            'name': config.KAFKA_TOPIC_PRODUCT_EVENTS,
            'num_partitions': 3,
            'replication_factor': 1
        },
        {
            'name': config.KAFKA_TOPIC_USER_EVENTS,
            'num_partitions': 2,
            'replication_factor': 1
        },
        {
            'name': config.KAFKA_TOPIC_PURCHASES,
            'num_partitions': 2,
            'replication_factor': 1
        }
    ]
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            client_id='topic-setup'
        )
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}CONFIGURACIÓN DE TOPICS DE KAFKA")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        # Crear topics
        new_topics = []
        for topic_config in topics:
            new_topic = NewTopic(
                name=topic_config['name'],
                num_partitions=topic_config['num_partitions'],
                replication_factor=topic_config['replication_factor']
            )
            new_topics.append(new_topic)
        
        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print(f"{Fore.GREEN}✓ Topics creados exitosamente:\n")
            for topic in topics:
                print(f"  • {topic['name']}")
                print(f"    - Particiones: {topic['num_partitions']}")
                print(f"    - Factor de replicación: {topic['replication_factor']}\n")
        
        except TopicAlreadyExistsError:
            print(f"{Fore.YELLOW}⚠ Los topics ya existen\n")
            for topic in topics:
                print(f"  • {topic['name']}")
        
        # Listar todos los topics
        print(f"\n{Fore.CYAN}Topics disponibles en Kafka:")
        existing_topics = admin_client.list_topics()
        for topic in sorted(existing_topics):
            print(f"  • {topic}")
        
        admin_client.close()
        print(f"\n{Fore.GREEN}{'='*80}")
        print(f"{Fore.GREEN}✓ Configuración completada")
        print(f"{Fore.GREEN}{'='*80}\n")
    
    except KafkaError as e:
        print(f"{Fore.RED}✗ Error conectando a Kafka: {e}")
        print(f"{Fore.YELLOW}⚠ Asegúrate de que Kafka esté corriendo con: docker-compose up -d")
        sys.exit(1)


if __name__ == '__main__':
    create_topics()