import random
import string

def gerar_string_aleatoria(tamanho=8):
    letras = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letras) for _ in range(tamanho))

def gerar_1m_hostnames(nome_arquivo="hostnames.txt"):
    dominios_base = ["google.com", "uol.com.br", "github.com", "aws.amazon.com", 
                     "microsoft.com", "apple.com", "netflix.com", "facebook.com",
                     "twitter.com", "instagram.com", "linkedin.com", "globo.com"]
    
    sufixos = [".com", ".com.br", ".net", ".org", ".io", ".gov", ".edu"]
    
    print(f"Gerando 1.000.000 de hostnames em {nome_arquivo}...")
    
    with open(nome_arquivo, "w", encoding="utf-8") as f:
        # 1. Adiciona alguns wildcards específicos para teste
        for d in dominios_base:
            f.write(f"*.{d}\n")
            f.write(f"{d}\n")
            
        # 2. Gera o restante até completar 1 milhão
        for i in range(1000000 - (len(dominios_base) * 2)):
            # Alterna entre subdomínios, domínios aleatórios e alguns wildcards
            chance = random.random()
            
            if chance < 0.10: # 10% de chance de ser um wildcard novo
                hostname = f"*.{gerar_string_aleatoria(10)}{random.choice(sufixos)}"
            elif chance < 0.40: # 30% de chance de ser um subdomínio aleatório
                hostname = f"{gerar_string_aleatoria(5)}.{random.choice(dominios_base)}"
            else: # 60% de chance de ser um domínio aleatório comum
                hostname = f"{gerar_string_aleatoria(12)}{random.choice(sufixos)}"
                
            f.write(f"{hostname}\n")
            
            if i % 100000 == 0:
                print(f"{i} hostnames gerados...")

    print("✅ Arquivo de 1 milhão de hostnames pronto!")

if __name__ == "__main__":
    gerar_1m_hostnames()