# Importe les modules nécessaires
from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import datetime
import ssl

# Désactive la vérification SSL (non recommandé dans un environnement de production)
ssl._create_default_https_context = ssl._create_unverified_context

# URL de préfixe pour les données de voyage
prefix_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

# Fonction principale du script
def main():
    grab_data()

# Fonction pour obtenir le nom de fichier en fonction de l'année et du mois
def get_filename(years: int, month: int):
    return f"yellow_tripdata_{years}-{'0' if month < 10 else ''}{month}.parquet"

# Fonction principale pour récupérer les données
def grab_data() -> None:
    # Obtient le répertoire du script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Définit le répertoire de sortie pour les données brutes
    output_directory = os.path.join(script_dir, '..', '..', 'data', 'raw')

    # Configuration du client Minio
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    # Nom du bucket Minio
    bucket_name = "sceau"

    # Vérifie si le bucket existe déjà
    found = client.bucket_exists(bucket_name)
    if not found:
        # Crée le bucket s'il n'existe pas
        client.make_bucket(bucket_name)
        print("Bucket {} créé avec succès.".format(bucket_name))
    else:
        print("Le bucket {} existe déjà.".format(bucket_name))

    # Boucle sur les années et les mois
    for years in range(2023, 2024):
        for month in range(1, 13):
            # Obtient le nom de fichier
            filename = get_filename(years, month)
            # Construit l'URL complète
            url = f"{prefix_url}/{filename}"

            # Chemin de destination pour sauvegarder le fichier
            destination_path = os.path.join(output_directory, filename)
            print(url)
            try:
                # Télécharge le fichier depuis l'URL
                urllib.request.urlretrieve(url, destination_path)
            except Exception as e:
                print(e)
                exit(-1)
            # Met les données dans le bucket Minio
            client.fput_object(bucket_name, filename, destination_path)
            print("Réussi")

# Point d'entrée du script
if __name__ == '__main__':
    sys.exit(main())
