import os
import pandas as pd


# ============================================================
# CHEMINS
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "dataset.csv")


# ============================================================
# LECTURE DES FICHIERS OLIST
# ============================================================

print("Chargement des fichiers Olist...")

orders = pd.read_csv(
    os.path.join(RAW_DIR, "olist_orders_dataset.csv")
)

order_items = pd.read_csv(
    os.path.join(RAW_DIR, "olist_order_items_dataset.csv")
)

customers = pd.read_csv(
    os.path.join(RAW_DIR, "olist_customers_dataset.csv")
)

products = pd.read_csv(
    os.path.join(RAW_DIR, "olist_products_dataset.csv")
)

categories = pd.read_csv(
    os.path.join(RAW_DIR, "product_category_name_translation.csv")
)


# ============================================================
# 1. COMMANDES + CLIENTS
# ============================================================

df = orders.merge(
    customers,
    on="customer_id",
    how="left"
)


# ============================================================
# 2. AJOUT DES ARTICLES COMMANDÉS
# ============================================================

df = df.merge(
    order_items,
    on="order_id",
    how="inner"
)


# ============================================================
# 3. AJOUT DES INFORMATIONS PRODUITS
# ============================================================

df = df.merge(
    products,
    on="product_id",
    how="left"
)


# ============================================================
# 4. TRADUCTION DES CATÉGORIES
# ============================================================

df = df.merge(
    categories,
    on="product_category_name",
    how="left"
)


# ============================================================
# 5. CONSTRUCTION DU DATASET FINAL
# ============================================================

final_df = pd.DataFrame()

final_df["order_id"] = df["order_id"]

final_df["client_id"] = df["customer_unique_id"]

final_df["date"] = df["order_purchase_timestamp"]

final_df["product"] = df["product_id"]

final_df["category"] = (
    df["product_category_name_english"]
    .fillna(df["product_category_name"])
    .fillna("unknown")
)

# Une ligne dans order_items représente un article vendu
final_df["quantity"] = 1

final_df["unit_price"] = pd.to_numeric(
    df["price"],
    errors="coerce"
)

final_df["amount"] = (
        final_df["quantity"]
        * final_df["unit_price"]
)

final_df["city"] = df["customer_city"]

final_df["region"] = df["customer_state"]


# ============================================================
# 6. CONVERSION DE LA DATE
# ============================================================

final_df["date"] = pd.to_datetime(
    final_df["date"],
    errors="coerce"
)


# ============================================================
# INFORMATIONS
# ============================================================

print("----------------------------------------")
print("Dataset créé")
print("----------------------------------------")

print(f"Nombre de lignes : {len(final_df)}")

print(
    f"Nombre de commandes : "
    f"{final_df['order_id'].nunique()}"
)

print(
    f"Nombre de clients : "
    f"{final_df['client_id'].nunique()}"
)

print(
    f"Nombre de produits : "
    f"{final_df['product'].nunique()}"
)

print(
    f"Nombre de catégories : "
    f"{final_df['category'].nunique()}"
)


# ============================================================
# SAUVEGARDE
# ============================================================

final_df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8"
)

print("----------------------------------------")
print(f"Fichier généré : {OUTPUT_FILE}")
print("----------------------------------------")
