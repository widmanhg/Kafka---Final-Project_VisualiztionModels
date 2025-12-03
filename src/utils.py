"""
Utilidades del proyecto
Funciones helper para cargar datos y generar eventos
"""
import json
import uuid
from datetime import datetime
from typing import Dict, List
from config import PRODUCTS_FILE, USERS_FILE


def load_json_file(filepath: str) -> List[Dict]:
    """
    Carga un archivo JSON y retorna su contenido como lista de diccionarios
    
    Args:
        filepath: Ruta del archivo JSON
        
    Returns:
        Lista de diccionarios con los datos del archivo
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: Archivo {filepath} no encontrado")
        return []
    except json.JSONDecodeError:
        print(f"Error: Archivo {filepath} no es un JSON válido")
        return []


def load_products() -> List[Dict]:
    """Carga el archivo de productos"""
    return load_json_file(PRODUCTS_FILE)


def load_users() -> List[Dict]:
    """Carga el archivo de usuarios"""
    return load_json_file(USERS_FILE)


def create_event(event_type: str, user: Dict, product: Dict) -> Dict:
    """
    Crea un evento estructurado para Kafka
    
    Args:
        event_type: Tipo de evento (view, add_to_cart, etc.)
        user: Diccionario con información del usuario
        product: Diccionario con información del producto
        
    Returns:
        Diccionario con la estructura del evento
    """
    return {
        'event_id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'event_type': event_type,
        'user_id': user['user_id'],
        'user_region': user['region'],
        'user_type': user['customer_type'],
        'product_id': product['product_id'],
        'product_name': product['name'],
        'product_category': product['category'],
        'product_price': product['price']
    }


def format_event_for_display(event: Dict) -> str:
    """
    Formatea un evento para mostrar en consola
    
    Args:
        event: Diccionario del evento
        
    Returns:
        String formateado del evento
    """
    return (f"[{event['timestamp']}] "
            f"{event['event_type'].upper()} - "
            f"User: {event['user_id']} | "
            f"Product: {event['product_name']} | "
            f"Region: {event['user_region']}")