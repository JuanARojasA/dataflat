class FlatteningException(Exception):
    """Exception raised for errors during the dictionary flattening process.

        Parameters
        ----------
        message : str
            Explanation of the error.
    """

    def __init__(self, message:str):
        self.message = message
        super().__init__(self.message)