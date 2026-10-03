from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import os
import pandas as pd
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
import json
from pymongo import MongoClient

# ============================================================
# CONFIGURATION
# ============================================================

DATA_FILE = "/opt/airflow/data/dataset.csv"
REPORT_FILE = "/opt/airflow/reports/ecommerce_report.json"
VALID_FILE = "/opt/airflow/data/processed/dataset_valid.csv"
ERROR_FILE = "/opt/airflow/data/errors/errors.csv"

REQUIRED_COLUMNS = [
    "order_id",
    "client_id",
    "date",
    "product",
    "category",
    "quantity",
    "unit_price",
    "amount",
    "city",
    "region",
]


# ============================================================
# 1. VALIDATION DU FICHIER
# ============================================================

def validate_file(**context):

    ti = context["ti"]

    if not os.path.exists(DATA_FILE):
        print("Le fichier dataset.csv n'existe pas.")

        ti.xcom_push(
            key="file_valid",
            value=False
        )

        return

    if os.path.getsize(DATA_FILE) == 0:
        print("Le fichier dataset.csv est vide.")

        ti.xcom_push(
            key="file_valid",
            value=False
        )

        return

    df = pd.read_csv(DATA_FILE)

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Colonnes manquantes : {missing_columns}"
        )

    print("========================================")
    print("VALIDATION DU FICHIER")
    print("========================================")
    print(f"Fichier : {DATA_FILE}")
    print(f"Nombre de lignes : {len(df)}")
    print("Toutes les colonnes obligatoires sont présentes.")

    ti.xcom_push(
        key="file_valid",
        value=True
    )

    ti.xcom_push(
        key="total_rows",
        value=len(df)
    )


# ============================================================
# 2. CONTRÔLE QUALITÉ
# ============================================================

def check_data_quality(**context):

    ti = context["ti"]

    df = pd.read_csv(DATA_FILE)

    # --------------------------------------------------------
    # Conversion des types
    # --------------------------------------------------------

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    df["unit_price"] = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Construction des règles d'erreur
    # --------------------------------------------------------

    df["error_reason"] = ""

    # Identifiants obligatoires
    missing_id = (
            df["order_id"].isna()
            | df["client_id"].isna()
            | df["product"].isna()
    )

    df.loc[
        missing_id,
        "error_reason"
    ] += "identifiant_manquant;"

    # Date invalide
    invalid_date = df["date"].isna()

    df.loc[
        invalid_date,
        "error_reason"
    ] += "date_invalide;"

    # Quantité <= 0
    invalid_quantity = (
            df["quantity"].isna()
            | (df["quantity"] <= 0)
    )

    df.loc[
        invalid_quantity,
        "error_reason"
    ] += "quantite_invalide;"

    # Montant négatif
    invalid_amount = (
            df["amount"].isna()
            | (df["amount"] < 0)
    )

    df.loc[
        invalid_amount,
        "error_reason"
    ] += "montant_invalide;"

    # Prix invalide
    invalid_price = (
            df["unit_price"].isna()
            | (df["unit_price"] < 0)
    )

    df.loc[
        invalid_price,
        "error_reason"
    ] += "prix_invalide;"

    # --------------------------------------------------------
    # Séparation lignes valides / invalides
    # --------------------------------------------------------

    invalid_mask = df["error_reason"] != ""

    invalid_df = df[invalid_mask].copy()

    valid_df = df[~invalid_mask].copy()

    # Supprimer error_reason du dataset valide
    valid_df = valid_df.drop(
        columns=["error_reason"]
    )

    # --------------------------------------------------------
    # Création des dossiers
    # --------------------------------------------------------

    os.makedirs(
        os.path.dirname(VALID_FILE),
        exist_ok=True
    )

    os.makedirs(
        os.path.dirname(ERROR_FILE),
        exist_ok=True
    )

    # --------------------------------------------------------
    # Sauvegarde
    # --------------------------------------------------------

    valid_df.to_csv(
        VALID_FILE,
        index=False
    )

    invalid_df.to_csv(
        ERROR_FILE,
        index=False
    )

    valid_rows = len(valid_df)
    invalid_rows = len(invalid_df)

    print("========================================")
    print("CONTRÔLE QUALITÉ")
    print("========================================")
    print(f"Lignes totales   : {len(df)}")
    print(f"Lignes valides   : {valid_rows}")
    print(f"Lignes rejetées  : {invalid_rows}")
    print(f"Fichier valide   : {VALID_FILE}")
    print(f"Fichier erreurs  : {ERROR_FILE}")

    # --------------------------------------------------------
    # XCom
    # --------------------------------------------------------

    ti.xcom_push(
        key="valid_rows",
        value=valid_rows
    )

    ti.xcom_push(
        key="invalid_rows",
        value=invalid_rows
    )

    ti.xcom_push(
        key="valid_file",
        value=VALID_FILE
    )

    ti.xcom_push(
        key="error_file",
        value=ERROR_FILE
    )


# ============================================================
# 3. BRANCHING
# ============================================================

def choose_processing_path(**context):

    ti = context["ti"]

    valid_rows = ti.xcom_pull(
        task_ids="check_data_quality",
        key="valid_rows"
    )

    print(
        f"Nombre de lignes valides reçu via XCom : "
        f"{valid_rows}"
    )

    if valid_rows and valid_rows > 0:

        print("Des données valides sont disponibles.")
        print("Le traitement continue.")

        return "continue_processing"

    print("Aucune donnée valide.")
    print("Le pipeline s'arrête proprement.")

    return "stop_processing"

# ============================================================
# 4. CHARGEMENT DES DONNÉES VALIDES
# ============================================================

def load_valid_data(**context):

    ti = context["ti"]

    valid_file = ti.xcom_pull(
        task_ids="check_data_quality",
        key="valid_file"
    )

    if not valid_file or not os.path.exists(valid_file):
        raise FileNotFoundError(
            "Le fichier de données valides est introuvable."
        )

    df = pd.read_csv(valid_file)

    print("========================================")
    print("CHARGEMENT DES DONNÉES VALIDES")
    print("========================================")
    print(f"Fichier chargé : {valid_file}")
    print(f"Nombre de lignes : {len(df)}")

    ti.xcom_push(
        key="loaded_rows",
        value=len(df)
    )

    ti.xcom_push(
        key="loaded_file",
        value=valid_file
    )


# ============================================================
# 5. CALCUL DES INDICATEURS MÉTIER
# ============================================================

def calculate_business_metrics(**context):

    ti = context["ti"]

    valid_file = ti.xcom_pull(
        task_ids="load_valid_data",
        key="loaded_file"
    )

    df = pd.read_csv(valid_file)

    # --------------------------------------------------------
    # Conversions
    # --------------------------------------------------------

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # KPI GLOBAUX
    # --------------------------------------------------------

    nb_commandes = int(
        df["order_id"].nunique()
    )

    nb_clients = int(
        df["client_id"].nunique()
    )

    chiffre_affaires = float(
        df["amount"].sum()
    )

    if nb_commandes > 0:
        panier_moyen = float(
            chiffre_affaires / nb_commandes
        )
    else:
        panier_moyen = 0.0

    # --------------------------------------------------------
    # TOP 10 PRODUITS
    # --------------------------------------------------------

    top_products_df = (
        df.groupby("product", as_index=False)
        .agg(
            sales=("quantity", "sum"),
            revenue=("amount", "sum")
        )
        .sort_values(
            by="sales",
            ascending=False
        )
        .head(10)
    )

    top_products = []

    for _, row in top_products_df.iterrows():
        top_products.append({
            "product": str(row["product"]),
            "sales": int(row["sales"]),
            "revenue": float(row["revenue"])
        })

    # --------------------------------------------------------
    # CHIFFRE D'AFFAIRES PAR CATÉGORIE
    # --------------------------------------------------------

    category_df = (
        df.groupby("category", as_index=False)
        ["amount"]
        .sum()
        .sort_values(
            by="amount",
            ascending=False
        )
    )

    category_metrics = []

    for _, row in category_df.iterrows():
        category_metrics.append({
            "category": str(row["category"]),
            "revenue": float(row["amount"])
        })

    # --------------------------------------------------------
    # CHIFFRE D'AFFAIRES PAR RÉGION
    # --------------------------------------------------------

    region_df = (
        df.groupby("region", as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            revenue=("amount", "sum")
        )
        .sort_values(
            by="revenue",
            ascending=False
        )
    )

    region_metrics = []

    for _, row in region_df.iterrows():
        region_metrics.append({
            "region": str(row["region"]),
            "orders": int(row["orders"]),
            "revenue": float(row["revenue"])
        })

    # --------------------------------------------------------
    # ÉVOLUTION DES VENTES PAR MOIS
    # --------------------------------------------------------

    df["month"] = (
        df["date"]
        .dt.to_period("M")
        .astype(str)
    )

    monthly_df = (
        df.groupby("month", as_index=False)
        ["amount"]
        .sum()
        .sort_values("month")
    )

    monthly_sales = []

    for _, row in monthly_df.iterrows():
        monthly_sales.append({
            "month": str(row["month"]),
            "revenue": float(row["amount"])
        })

    # --------------------------------------------------------
    # QUALITÉ
    # --------------------------------------------------------

    valid_rows = ti.xcom_pull(
        task_ids="check_data_quality",
        key="valid_rows"
    )

    invalid_rows = ti.xcom_pull(
        task_ids="check_data_quality",
        key="invalid_rows"
    )

    # --------------------------------------------------------
    # LOGS
    # --------------------------------------------------------

    print("========================================")
    print("INDICATEURS MÉTIER")
    print("========================================")
    print(f"Nombre de commandes : {nb_commandes}")
    print(f"Nombre de clients : {nb_clients}")
    print(
        f"Chiffre d'affaires : "
        f"{chiffre_affaires:.2f}"
    )
    print(
        f"Panier moyen : "
        f"{panier_moyen:.2f}"
    )
    print(f"Lignes valides : {valid_rows}")
    print(f"Lignes rejetées : {invalid_rows}")

    print("========================================")
    print("TOP 10 PRODUITS")
    print("========================================")

    for product in top_products:
        print(product)

    # --------------------------------------------------------
    # XCOM
    # --------------------------------------------------------

    ti.xcom_push(
        key="nb_commandes",
        value=nb_commandes
    )

    ti.xcom_push(
        key="nb_clients",
        value=nb_clients
    )

    ti.xcom_push(
        key="chiffre_affaires",
        value=chiffre_affaires
    )

    ti.xcom_push(
        key="panier_moyen",
        value=panier_moyen
    )

    ti.xcom_push(
        key="top_products",
        value=top_products
    )

    ti.xcom_push(
        key="category_metrics",
        value=category_metrics
    )

    ti.xcom_push(
        key="region_metrics",
        value=region_metrics
    )

    ti.xcom_push(
        key="monthly_sales",
        value=monthly_sales
    )

@task
def get_categories():

    df = pd.read_csv(VALID_FILE)

    categories = sorted(
        df["category"]
        .dropna()
        .unique()
        .tolist()
    )

    print(f"Nombre de catégories : {len(categories)}")

    for category in categories:
        print(category)

    return categories


@task
def analyze_category(category):
    # Erreur volontaire pour tester la gestion des erreurs
    if category == "audio":
        raise ValueError(
            "Erreur simulée volontairement pour la catégorie audio"
        )

    df = pd.read_csv(VALID_FILE)

    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    category_df = df[
        df["category"] == category
        ].copy()

    nb_commandes = int(
        category_df["order_id"].nunique()
    )

    quantity_sold = int(
        category_df["quantity"].sum()
    )

    revenue = float(
        category_df["amount"].sum()
    )

    result = {
        "category": category,
        "orders": nb_commandes,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }

    print("====================================")
    print(f"CATÉGORIE : {category}")
    print("====================================")
    print(f"Commandes : {nb_commandes}")
    print(f"Quantité vendue : {quantity_sold}")
    print(f"Chiffre d'affaires : {revenue:.2f}")

    return result

# ============================================================
# 6. GÉNÉRATION DU RAPPORT FINAL
# ============================================================

def generate_final_report(**context):

    ti = context["ti"]

    # ========================================================
    # KPI GLOBAUX
    # ========================================================

    nb_commandes = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="nb_commandes"
    )

    nb_clients = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="nb_clients"
    )

    chiffre_affaires = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="chiffre_affaires"
    )

    panier_moyen = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="panier_moyen"
    )

    top_products = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="top_products"
    )

    category_metrics = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="category_metrics"
    )

    region_metrics = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="region_metrics"
    )

    monthly_sales = ti.xcom_pull(
        task_ids="calculate_business_metrics",
        key="monthly_sales"
    )

    # ========================================================
    # QUALITÉ DES DONNÉES
    # ========================================================

    valid_rows = ti.xcom_pull(
        task_ids="check_data_quality",
        key="valid_rows"
    )

    invalid_rows = ti.xcom_pull(
        task_ids="check_data_quality",
        key="invalid_rows"
    )

    error_file = ti.xcom_pull(
        task_ids="check_data_quality",
        key="error_file"
    )

    # ========================================================
    # RÉSULTATS DES TÂCHES DYNAMIQUES
    # ========================================================

    category_results = ti.xcom_pull(
        task_ids="analyze_category",
        key="return_value"
    )

    if category_results is None:
        category_results = []

    valid_category_results = [
        result
        for result in category_results
        if isinstance(result, dict)
    ]

    expected_categories = ti.xcom_pull(
        task_ids="get_categories",
        key="return_value"
    )

    if expected_categories is None:
        expected_categories = []

    # ========================================================
    # STATUT DU TRAITEMENT
    # ========================================================

    if len(valid_category_results) == len(expected_categories):
        status = "success"
    elif len(valid_category_results) > 0:
        status = "partial"
    else:
        status = "failed"

    # ========================================================
    # CONSTRUCTION DU RAPPORT
    # ========================================================

    report = {
        "execution_date": context["logical_date"].isoformat(),

        "dag_id": context["dag"].dag_id,

        "dataset": "olist",

        "source_file": "dataset.csv",

        "status": status,

        "global_metrics": {
            "nb_commandes": nb_commandes,
            "nb_clients": nb_clients,
            "chiffre_affaires": chiffre_affaires,
            "panier_moyen": panier_moyen
        },

        "top_products": top_products,

        "category_metrics": category_metrics,

        "region_metrics": region_metrics,

        "monthly_sales": monthly_sales,

        "category_analysis": valid_category_results,

        "quality": {
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "error_file": error_file
        }
    }

    # ========================================================
    # SAUVEGARDE
    # ========================================================

    os.makedirs(
        os.path.dirname(REPORT_FILE),
        exist_ok=True
    )

    with open(
            REPORT_FILE,
            "w",
            encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=4,
            ensure_ascii=False
        )

    print("========================================")
    print("RAPPORT FINAL GÉNÉRÉ")
    print("========================================")
    print(f"Statut : {status}")
    print(f"Fichier : {REPORT_FILE}")
    print(
        f"Analyses catégories réussies : "
        f"{len(valid_category_results)} "
        f"/ {len(expected_categories)}"
    )

    # Le rapport servira ensuite à MongoDB
    ti.xcom_push(
        key="report",
        value=report
    )

    ti.xcom_push(
        key="status",
        value=status
    )

    # ============================================================
# 7. STOCKAGE DANS MONGODB
# ============================================================

def save_metrics_to_mongodb(**context):

    ti = context["ti"]

    # Récupérer le rapport final depuis XCom
    report = ti.xcom_pull(
        task_ids="generate_final_report",
        key="report"
    )

    if not report:
        raise ValueError(
            "Aucun rapport disponible pour MongoDB."
        )

    # Connexion au service MongoDB Docker
    client = MongoClient(
        "mongodb://mongodb:27017",
        serverSelectionTimeoutMS=5000
    )

    # Vérifier la connexion
    client.admin.command("ping")

    # Base et collection demandées
    db = client["ecommerce_analytics"]
    collection = db["sales_metrics"]

    # Ajouter quelques informations utiles
    report["mongodb_inserted_at"] = datetime.utcnow().isoformat()

    # Historiser chaque exécution
    result = collection.insert_one(report)

    print("========================================")
    print("MÉTRIQUES ENREGISTRÉES DANS MONGODB")
    print("========================================")
    print(f"ID MongoDB : {result.inserted_id}")
    print("Base : ecommerce_analytics")
    print("Collection : sales_metrics")
    print(f"Statut : {report['status']}")

    client.close()
# ============================================================
# DÉFINITION DU DAG
# ============================================================

with DAG(
        dag_id="ecommerce_sales_pipeline",
        description="Pipeline Data Engineering E-commerce Olist",
        start_date=datetime(2026, 10, 3),
        schedule=None,
        catchup=False,
        tags=["ecommerce", "data-engineering", "olist"],
) as dag:

    # ========================================================
    # TÂCHE 1 : attendre le fichier
    # ========================================================

    wait_for_file = FileSensor(
        task_id="wait_for_dataset",

        fs_conn_id="fs_default",

        # fs_default pointe sur /opt/airflow/data
        filepath="dataset.csv",

        poke_interval=10,

        timeout=300,
    )


    # ========================================================
    # TÂCHE 2 : vérifier le fichier
    # ========================================================

    validate_file_task = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
    )


    # ========================================================
    # TÂCHE 3 : contrôle qualité
    # ========================================================

    quality_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )


    # ========================================================
    # TÂCHE 4 : choix du chemin
    # ========================================================

    branch_task = BranchPythonOperator(
        task_id="choose_processing_path",
        python_callable=choose_processing_path,
    )


    # ========================================================
    # TÂCHE 5A : poursuivre
    # ========================================================

    continue_task = EmptyOperator(
        task_id="continue_processing"
    )


    # ========================================================
    # TÂCHE 5B : arrêter
    # ========================================================

    stop_task = EmptyOperator(
        task_id="stop_processing"
    )

    # ========================================================
    # TÂCHE 6 : chargement des données valides
    # ========================================================

    load_data_task = PythonOperator(
        task_id="load_valid_data",
        python_callable=load_valid_data,
    )


    # ========================================================
    # TÂCHE 7 : calcul des KPI
    # ========================================================

    metrics_task = PythonOperator(
        task_id="calculate_business_metrics",
        python_callable=calculate_business_metrics,
    )

    categories = get_categories()

    category_analyses = analyze_category.expand(
        category=categories
    )
    analysis_complete = EmptyOperator(
        task_id="analysis_complete",
        trigger_rule=TriggerRule.ALL_DONE
    )

    final_report = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    save_mongodb = PythonOperator(
        task_id="save_metrics_to_mongodb",
        python_callable=save_metrics_to_mongodb,
    )

    # ========================================================
    # DÉPENDANCES
    # ========================================================

    wait_for_file \
    >> validate_file_task \
    >> quality_task \
    >> branch_task

    branch_task >> [
        continue_task,
        stop_task
    ]
    continue_task >> load_data_task >> metrics_task
    metrics_task >> categories
    category_analyses >> analysis_complete
    category_analyses >> analysis_complete
    analysis_complete >> final_report
    final_report >> save_mongodb
