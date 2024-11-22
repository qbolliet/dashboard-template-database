# Importation des modules
# Modules de base
import os
# Logging
import logging

# Fonction auxiliaire d'initialisation d'un logger
def _init_logger(filename: os.PathLike) -> logging.Logger:
    """
    Initializes a logger.

    Configures the logging format, level, and file handler.
    Creates the directory for the log file if it does not exist.

    Args:
        filename (os.PathLike):
            The path to the log file.

    Returns:
        logging.Logger: The initialized logger.
    """
    # Configuration du logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
        level=logging.INFO,
    )

    # Vérification de l'existence du dossier pour le fichier de log
    log_directory = os.path.dirname(filename)
    # Création du chemin s'il n'existe pas déjà
    if (not os.path.exists(log_directory)) & (log_directory != ""):
        os.makedirs(log_directory)

    # Configuration du fichier de logs
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)

    # Initialisation du logger
    logger = logging.getLogger()
    logger.addHandler(file_handler)

    return logger