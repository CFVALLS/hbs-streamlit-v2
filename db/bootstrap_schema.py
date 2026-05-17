"""
Bootstrap schema creation for a fresh database using SQLAlchemy models.
Intended for local/dev (e.g., Laragon MySQL). It will create missing tables,
but will not alter existing ones or add constraints to existing tables.
"""
from db.models_orm import Base
from db.connection_db import establecer_engine


def main():
    engine = establecer_engine()
    try:
        Base.metadata.create_all(engine)
        print("✅ Schema created/verified.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
