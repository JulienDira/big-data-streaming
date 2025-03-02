import requests
import json
import time
from kafka import KafkaProducer
import threading

# Configuration Kafka
topic_name = 'longtime'
servers = 'kafka:9092'
LIMIT = 30

# Attendre que Kafka soit prêt
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=[servers])
        print("Connexion à Kafka établie")
        break
    except Exception as e:
        print(f"Tentative de connexion à Kafka: {e}")
        time.sleep(1)

# Convertir les valeurs numériques en float
def convert_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return value

# Récupérer et envoyer les données pour une paire et un intervalle spécifiques
def process_coin_interval(coin, interval):
    try:
        # Récupérer les données
        url = f'https://api.binance.com/api/v3/klines?symbol={coin}&interval={interval}'
        r = requests.get(url)
        data = r.json()
        print(f"Données obtenues pour {coin}/{interval}: {len(data)} entrées")
        
        # Envoyer immédiatement les données à Kafka
        messages_sent = 0
        for entry in data:
            # Convertir la liste en dictionnaire avec des types numériques
            entry_dict = {
                "coin": coin,
                "timestamp": entry[0],
                "open": convert_to_float(entry[1]),
                "high": convert_to_float(entry[2]),
                "low": convert_to_float(entry[3]),
                "close": convert_to_float(entry[4]),
                "volume": convert_to_float(entry[5]),
                "close_time": entry[6],
                "quote_asset_volume": convert_to_float(entry[7]),
                "number_of_trades": int(entry[8]),
                "taker_buy_base_asset_volume": convert_to_float(entry[9]),
                "taker_buy_quote_asset_volume": convert_to_float(entry[10]),
                "ignore": entry[11],
                "interval": interval
            }
            
            # Envoyer le message
            producer.send(topic_name, json.dumps(entry_dict).encode('utf-8'))
            messages_sent += 1
        
        # Flush après chaque coin/intervalle
        producer.flush()
        print(f"Envoyé {messages_sent} messages pour {coin}/{interval}")
        return messages_sent
    
    except Exception as e:
        print(f"Erreur pour {coin}/{interval}: {e}")
        return 0

# Liste des cryptomonnaies à surveiller
def getCoinsList():
    return ['BTCUSDC', 'ETHUSDC', 'XRPUSDC', 'SOLUSDC']

# Liste des intervalles à collecter
intervals = ['1m', '5m', '15m', '1h', '1d']

# Fonction pour traiter plusieurs paires/intervalles avec multithreading
def process_all_coins_intervals(coins, intervals):
    threads = []
    for coin in coins:
        for interval in intervals:
            # Créer un thread pour chaque combinaison coin/interval
            print(f"Début du traitement pour {coin} avec l'intervalle {interval}")
            thread = threading.Thread(target=process_coin_interval, args=(coin, interval))
            threads.append(thread)
            thread.start()
    
    # Attendre que tous les threads terminent
    total_messages = 0
    for thread in threads:
        thread.join()
        print(f"Thread {thread} terminé")
    
    return len(coins) * len(intervals) * LIMIT  # Estimation du nombre de messages

# Boucle principale avec gestion des erreurs
def main():
    coin_list = getCoinsList()
    cycle = 0
    
    while True:
        try:
            cycle += 1
            start_time = time.time()
            print(f"\n--- Début du cycle {cycle} ---")
            
            # Traiter toutes les paires/intervalles en parallèle
            estimated_messages = process_all_coins_intervals(coin_list, intervals)
            
            # Calculer le temps d'exécution
            execution_time = time.time() - start_time
            print(f"Cycle {cycle} terminé en {execution_time:.2f} secondes. Environ {estimated_messages} messages envoyés.")
            
        except Exception as e:
            print(f"Erreur dans le cycle principal: {e}")
            time.sleep(30)  # Attendre en cas d'erreur

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Programme interrompu par l'utilisateur")
        producer.close()
        print("Producteur Kafka fermé")