import pandas as pd
import mysql.connector
import os
from config import MYSQL_CONFIG, extract_filepath, transform_filepath
from sql_queries import create_table_query, insert_data_query
from airflow.models import TaskInstance

def create_table():
    """ 
    Crée la table 'orders' si elle n'existe pas déjà dans MySQL
    Utilise la requête create_table_query importée depuis `sql_queries.py` 
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table a été créée avec succès !")
    except mysql.connector.Error as err:
        print(f'La connexion a échoué : {err}')

def extract() -> dict | None:
    """ 
    Extrait les données du fichier CSV avec le filepath défini dans `config.py`
    Convertit les données en dictionnaire, compatible avec XCom
    Retourne :
    - `dict` : Liste de dictionnaires représentant les données extraites. 
    - `None` : Si le fichier est introuvable ou en cas d'erreur de lecture
    """
    if os.path.exists(extract_filepath):
        try:
            df = pd.read_csv(extract_filepath)
            print(f'Le fichier {extract_filepath} a été correctement extrait')
            return df.to_dict(orient="records")
        except Exception as e:
            print(f'Erreur lors de la lecture du fichier {extract_filepath} : {e}')
            return None
    else:
        print(f'Le fichier {extract_filepath} est introuvable')
        return None

def transform(ti: TaskInstance) -> str | None:
    """ 
   `ti` (TaskInstance) instance airflow qui permet de récupérer le dictionnaire stocké dans XCom.
    Le convertit en dataframe pour effectuer les transformations avec pandas :
    - Vérifie la présence des colonnes requises.
    - Remplace les valeurs NaN par la date actuelle.
    - Supprime les doublons.
    - Sauvegarde le DataFrame transformé dans un fichier CSV.
    Retourne :
    - `str` : Chemin du fichier transformé
    - `None` : En cas d'erreur ou si les colonnes requises sont invalides. 
    """
    data = ti.xcom_pull(task_ids='extract', key='return_value')

    if data is None:
        print("Erreur : Aucune donnée extraite.")
        return None

    df = pd.DataFrame(data)

    required_columns = {'order_purchase_timestamp', 'order_id'}
    if not required_columns.issubset(df.columns):
        print(f"Erreur : Les colonnes requises {required_columns} ne sont pas toutes présentes dans le DataFrame.")
        return None 

    try:
        df['order_purchase_timestamp'] = df['order_purchase_timestamp'].fillna(pd.Timestamp.now())
        df = df.dropna(subset=['order_id'])
        df = df.drop_duplicates(subset=['order_id'])
        df.to_csv(transform_filepath, index=False)
        print('Les données ont été transformées avec succès')
        return transform_filepath
    except Exception as e:
        print(f'Erreur lors de la transformation : {e}')
        return None


def load(ti: TaskInstance):
    """
    Charge les données transformées avec le filepath stocké dans XCom vers la base de données MySQL.
    - Vérifie que le fichier transformé existe.
    - Insère les données dans la table `orders`.
    - Gère les conflits avec `ON DUPLICATE KEY UPDATE`.

    Retourne :
    - Seulement en cas d'erreur
    """
    file = ti.xcom_pull(task_ids='transform', key='return_value')

    if not file or not os.path.exists(file): 
        print("Erreur : Fichier transformé introuvable.")
        return

    df = pd.read_csv(file, parse_dates=['order_purchase_timestamp'])
    print(df.dtypes)

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        required_columns = {'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp'}
        if not required_columns.issubset(df.columns):
            raise KeyError(f"Les colonnes requises {required_columns} ne sont pas toutes présentes dans le DataFrame.")

        for _, row in df.iterrows():
            if pd.isna(row['order_purchase_timestamp']):
                row['order_purchase_timestamp'] = pd.Timestamp.now()

            cursor.execute(insert_data_query, (
                row['order_id'], 
                row['customer_id'], 
                row['order_status'], 
                row['order_purchase_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print('Les données ont été chargées avec succès')
    except mysql.connector.Error as err:
        print(f'Erreur dans le chargement des données : {err}')
