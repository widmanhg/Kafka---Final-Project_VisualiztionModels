"""
Script para ejecutar múltiples producers simultáneamente
Simula tráfico concurrente en los 3 topics de Kafka
"""
import sys
import os
from multiprocessing import Process
from colorama import Fore, init

# Agregar el directorio src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from producer import ProductEventProducer
from purchase_producer import PurchaseProducer
from user_producer import UserEventProducer

init(autoreset=True)


def run_product_producer(num_events: int):
    """Ejecuta el producer de eventos de productos"""
    try:
        producer = ProductEventProducer()
        producer.run(num_events=num_events)
    except Exception as e:
        print(f"{Fore.RED}Error en Product Producer: {e}")


def run_purchase_producer(num_events: int):
    """Ejecuta el producer de compras"""
    try:
        producer = PurchaseProducer()
        producer.run(num_events=num_events)
    except Exception as e:
        print(f"{Fore.RED}Error en Purchase Producer: {e}")


def run_user_producer(num_events: int):
    """Ejecuta el producer de eventos de usuario"""
    try:
        producer = UserEventProducer()
        producer.run(num_events=num_events)
    except Exception as e:
        print(f"{Fore.RED}Error en User Producer: {e}")


def main():
    """Función principal que lanza múltiples producers para los 3 topics"""
    
    # Configuración
    num_events_product = 40
    num_events_purchase = 15
    num_events_user = 30
    
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.CYAN}EJECUTANDO MÚLTIPLES PRODUCERS SIMULTÁNEAMENTE")
    print(f"{Fore.CYAN}{'='*80}")
    print(f"{Fore.YELLOW}Configuración:")
    print(f"  • Product Events Topic  : {num_events_product} eventos")
    print(f"  • Purchases Topic       : {num_events_purchase} eventos")
    print(f"  • User Events Topic     : {num_events_user} eventos")
    print(f"{Fore.CYAN}{'='*80}\n")
    
    # Crear procesos para cada tipo de producer
    processes = []
    
    # Producer de eventos de productos
    product_process = Process(
        target=run_product_producer,
        args=(num_events_product,)
    )
    processes.append(product_process)
    product_process.start()
    print(f"{Fore.GREEN}✓ Product Event Producer iniciado (topic: product-events)")
    
    # Producer de compras
    purchase_process = Process(
        target=run_purchase_producer,
        args=(num_events_purchase,)
    )
    processes.append(purchase_process)
    purchase_process.start()
    print(f"{Fore.GREEN}✓ Purchase Producer iniciado (topic: purchases)")
    
    # Producer de eventos de usuario
    user_process = Process(
        target=run_user_producer,
        args=(num_events_user,)
    )
    processes.append(user_process)
    user_process.start()
    print(f"{Fore.GREEN}✓ User Event Producer iniciado (topic: user-events)")
    
    # Esperar a que todos terminen
    print(f"\n{Fore.YELLOW}Esperando que todos los producers terminen...\n")
    for process in processes:
        process.join()
    
    total_events = num_events_product + num_events_purchase + num_events_user
    print(f"\n{Fore.GREEN}{'='*80}")
    print(f"{Fore.GREEN}✓ TODOS LOS PRODUCERS HAN FINALIZADO")
    print(f"{Fore.GREEN}✓ Total de eventos generados: {total_events}")
    print(f"{Fore.GREEN}{'='*80}\n")


if __name__ == '__main__':
    main()