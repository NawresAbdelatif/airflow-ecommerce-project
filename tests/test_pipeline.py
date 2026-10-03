from pathlib import Path

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DATA_FILE = PROJECT_DIR / "data" / "dataset.csv"

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
# OUTIL COMMUN
# ============================================================

def load_dataset():
    return pd.read_csv(DATA_FILE)


# ============================================================
# TEST 1 : LE FICHIER EXISTE
# ============================================================

def test_dataset_exists():

    assert DATA_FILE.exists(), (
        f"Le fichier {DATA_FILE} n'existe pas."
    )


# ============================================================
# TEST 2 : LE FICHIER N'EST PAS VIDE
# ============================================================

def test_dataset_not_empty():

    df = load_dataset()

    assert len(df) > 0, (
        "Le dataset est vide."
    )


# ============================================================
# TEST 3 : COLONNES OBLIGATOIRES
# ============================================================

def test_required_columns():

    df = load_dataset()

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    assert not missing_columns, (
        f"Colonnes manquantes : {missing_columns}"
    )


# ============================================================
# TEST 4 : IDENTIFIANTS OBLIGATOIRES
# ============================================================

def test_required_identifiers_not_null():

    df = load_dataset()

    assert df["order_id"].notna().all()
    assert df["client_id"].notna().all()
    assert df["product"].notna().all()


# ============================================================
# TEST 5 : QUANTITÉS POSITIVES
# ============================================================

def test_quantity_is_positive():

    df = load_dataset()

    quantity = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    assert quantity.notna().all(), (
        "Certaines quantités ne sont pas numériques."
    )

    assert (quantity > 0).all(), (
        "Une quantité nulle ou négative a été détectée."
    )


# ============================================================
# TEST 6 : PRIX NON NÉGATIFS
# ============================================================

def test_unit_price_is_not_negative():

    df = load_dataset()

    prices = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    assert prices.notna().all()

    assert (prices >= 0).all(), (
        "Un prix négatif a été détecté."
    )


# ============================================================
# TEST 7 : MONTANTS NON NÉGATIFS
# ============================================================

def test_amount_is_not_negative():

    df = load_dataset()

    amounts = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    assert amounts.notna().all()

    assert (amounts >= 0).all(), (
        "Un montant négatif a été détecté."
    )


# ============================================================
# TEST 8 : COHÉRENCE DU MONTANT
# ============================================================

def test_amount_equals_quantity_times_price():

    df = load_dataset()

    quantity = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    price = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    amount = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    expected_amount = quantity * price

    difference = (
            amount - expected_amount
    ).abs()

    assert (difference < 0.001).all(), (
        "Certains montants sont incohérents."
    )


# ============================================================
# TEST 9 : DATES VALIDES
# ============================================================

def test_dates_are_valid():

    df = load_dataset()

    dates = pd.to_datetime(
        df["date"],
        errors="coerce"
    )

    assert dates.notna().all(), (
        "Certaines dates sont invalides."
    )


# ============================================================
# TEST 10 : KPI MINIMUMS
# ============================================================

def test_business_data_available():

    df = load_dataset()

    nb_commandes = df["order_id"].nunique()
    nb_clients = df["client_id"].nunique()

    chiffre_affaires = pd.to_numeric(
        df["amount"],
        errors="coerce"
    ).sum()

    assert nb_commandes > 0
    assert nb_clients > 0
    assert chiffre_affaires > 0
