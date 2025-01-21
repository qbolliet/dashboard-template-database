# Importation des modules
# Module de base
import os
import pandas as pd


# Classe de chargement des données
class Loader:
    """
    A class for loading data from various file formats into a pandas DataFrame.

    This utility class supports common file formats such as Excel (`.xlsx`, `.xls`), 
    CSV, Parquet, and Pickle files.
    """

    # Initialisation
    def __init__(self) -> None:
        """
        Initialize the Loader class.
        """
        pass

    # Méthode de chargement des données
    def load(self, path: os.PathLike, **kwargs) -> pd.DataFrame:
        """
        Load data from a file into a pandas DataFrame based on the file's extension.

        Supported file formats:
        - `.xlsx`: Excel files (using the `openpyxl` engine)
        - `.xls`: Excel files (using the `xlrd` engine)
        - `.csv`: CSV files
        - `.pkl`: Pickle files
        - `.parquet`: Parquet files

        Args:
            path (os.PathLike): Path to the file to load.
            **kwargs: Additional keyword arguments passed to the respective pandas loading function.

        Returns:
            pd.DataFrame: The data loaded into a pandas DataFrame.

        Raises:
            ValueError: If the file extension is unsupported.
        """

        # Extraction de l'extension du fichier à charger
        extension = path.split(".")[-1]

        # Test suivant l'extension du fichier à charger et lecture de ce-dernier
        if extension == "xlsx":
            data = pd.read_excel(path, engine="openpyxl", **kwargs)
        elif extension == "xls":
            data = pd.read_excel(path, engine="xlrd", **kwargs)
        elif extension == "csv":
            data = pd.read_csv(path, **kwargs)
        elif extension == "pkl":
            data = pd.read_pickle(path, **kwargs)
        elif extension == "parquet":
            data = pd.read_parquet(path, **kwargs)
        else:
            raise ValueError(
                "Invalid extension : should be in ['xlsx', 'xls', 'pkl', 'csv', 'parquet']."
            )

        return data
