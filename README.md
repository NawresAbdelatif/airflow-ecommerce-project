# Airflow E-commerce Data Engineering Project

## Présentation

Ce projet met en place une plateforme automatisée de traitement et d'analyse de données e-commerce.

L'objectif est d'industrialiser un pipeline de données permettant de :

- collecter et valider les données de ventes ;
- contrôler leur qualité ;
- isoler les données incorrectes ;
- calculer des indicateurs métier ;
- orchestrer les traitements avec Apache Airflow ;
- stocker les métriques dans MongoDB ;
- automatiser les tests et l'exécution du pipeline avec Jenkins ;
- versionner le projet avec Git et GitHub.

Le dataset utilisé est le **Brazilian E-Commerce Public Dataset by Olist**.

---

## Architecture

```text
GitHub
   |
   v
Jenkins
   |
   +--> Checkout
   |
   +--> Install dependencies
   |
   +--> Run tests
   |
   +--> Validate DAG
   |
   +--> Deploy DAG
   |
   +--> Trigger DAG
   |
   +--> Wait for Airflow
   |
   +--> Verify MongoDB
   |
   v
Apache Airflow
   |
   +--> FileSensor
   |
   +--> Validation du fichier
   |
   +--> Contrôle qualité
   |
   +--> Branching
   |
   +--> Chargement des données valides
   |
   +--> Calcul des KPI
   |
   +--> Analyse dynamique par catégorie
   |
   +--> Gestion des erreurs
   |
   +--> Génération du rapport
   |
   v
MongoDB
