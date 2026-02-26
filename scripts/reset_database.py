import argparse
import logging

from app.database import Base, engine


logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.reset_db")


def reset_database(drop_all: bool) -> None:
    if drop_all:
        logger.info("Dropping all tables...")
        Base.metadata.drop_all(bind=engine)
    logger.info("Creating tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database reset complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset database schema for GrandCru Value API.")
    parser.add_argument(
        "--drop-all",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Drop all existing tables before creating them again (default: true).",
    )
    args = parser.parse_args()
    reset_database(drop_all=args.drop_all)


if __name__ == "__main__":
    main()
