from collections import OrderedDict

class CacheL1:
    def __init__(self, capacidade_maxima: int):
        self.cache = OrderedDict()
        self.capacidade = capacidade_maxima

    def obter(self, hostname: str):
        if hostname not in self.cache:
            return None
        self.cache.move_to_end(hostname)
        return self.cache[hostname]

    def atualizar(self, hostname: str, status: str):
        if hostname in self.cache:
            self.cache.move_to_end(hostname)
        self.cache[hostname] = status
        if len(self.cache) > self.capacidade:
            self.cache.popitem(last=False)

# Instância global usada pelo app.py e workers.py
cache_duo = CacheL1(capacidade_maxima=100000)