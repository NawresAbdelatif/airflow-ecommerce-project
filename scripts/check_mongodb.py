import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError


# ============================================================
# CONFIGURATION
# ============================================================

MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://localhost:27019"
)

DATABASE_NAME = "ecommerce_analytics"
COLLECTION_NAME = "sales_metrics"


# ============================================================
# VÉRIFICATION MONGODB
# ============================================================

def check_mongodb():

    client = None

    try:
        print("========================================")
        print("VÉRIFICATION MONGODB")
        print("========================================")

        print(f"Connexion : {MONGODB_URI}")
        print(f"Base : {DATABASE_NAME}")
        print(f"Collection : {COLLECTION_NAME}")

        client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000
        )

        # Vérifier que MongoDB répond
        client.admin.command("ping")

        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Nombre de documents
        count = collection.count_documents({})

        print(f"Nombre de documents : {count}")

        if count == 0:
            print(
                "ERREUR : aucun document trouvé "
                "dans MongoDB."
            )
            sys.exit(1)

        # Dernier document enregistré
        document = collection.find_one(
            sort=[("_id", -1)]
        )

        if not document:
            print("ERREUR : impossible de récupérer le document.")
            sys.exit(1)

        print("----------------------------------------")
        print("Dernier document trouvé")
        print("----------------------------------------")

        print(f"DAG : {document.get('dag_id')}")
        print(f"Dataset : {document.get('dataset')}")
        print(f"Statut : {document.get('status')}")
        print(
            f"Date d'exécution : "
            f"{document.get('execution_date')}"
        )

        # ----------------------------------------------------
        # Vérification des champs obligatoires
        # ----------------------------------------------------

        required_fields = [
            "execution_date",
            "dag_id",
            "dataset",
            "source_file",
            "status",
            "global_metrics",
            "top_products",
            "region_metrics",
            "quality",
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in document
        ]

        if missing_fields:
            print(
                f"ERREUR : champs manquants : "
                f"{missing_fields}"
            )
            sys.exit(1)

        # ----------------------------------------------------
        # Vérification du statut
        # ----------------------------------------------------

        valid_statuses = [
            "success",
            "partial",
            "failed"
        ]

        if document["status"] not in valid_statuses:
            print(
                "ERREUR : statut MongoDB invalide."
            )
            sys.exit(1)

        print("----------------------------------------")
        print("MongoDB vérifié avec succès.")
        print("----------------------------------------")

        return 0

    except PyMongoError as error:

        print("ERREUR MONGODB :")
        print(error)

        return 1

    except Exception as error:

        print("ERREUR :")
        print(error)

        return 1

    finally:

        if client:
            client.close()


# ============================================================
# EXÉCUTION
# ============================================================

if __name__ == "__main__":

    sys.exit(
        check_mongodb()
    )
