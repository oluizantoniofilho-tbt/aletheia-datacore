class BaseLoader:
    """Classe base para todos os loaders."""
    
    def load(self):
        """Carrega dados brutos de alguma fonte externa."""
        raise NotImplementedError()