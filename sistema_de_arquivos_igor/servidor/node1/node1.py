import Pyro5.server
import Pyro5.core
import os
import shutil
import Pyro5.api
import base64
import threading
from pathlib import Path
import base64
import hashlib
import time

# endereço IP do servidor, substituir pelo ip da máquina que rodará o servidor
ip_servidor = '192.168.0.5'

#Endereço ip do name_server 
ip_name_server = '192.168.0.5'


#Antes de inicializar o script do servidor, deve ser criado um servidor de nomes manualmente.
#Para fazer isso, basta digitar no terminal "python -m Pyro5.nameserver -n hostname". Caso 
#não coloque hostname, o servidor de nomes será executado em localhost, e não será acessável
#por máquinas de fora. E para localizá-lo nos script de cliente e servidor, cria-se um proxy
#de servidor de nomes que localiza o servidor de nomes pelo endereço especificado ou por
#broadcast caso se tenha registrado o ip na lista "PYRO_BROADCAST_ADDRS". 

def ping(uri1):
    sistema_monitor=Pyro5.client.Proxy(uri1)
    print("Iniciando o envio periódico.")
    while True:
        # 1. Crie ou obtenha sua mensagem de bytes
        mensagem = 1
        c = sistema_monitor.enviar_ping(mensagem)
        print(f">>> Mensagem 'enviada' com sucesso: {c}")
        # 3. Pausa a execução por 30 segundos
        print("Aguardando 10 segundos para o próximo envio...")
        time.sleep(10)



#Identifica a classe como remota
@Pyro5.server.expose
class sist_arq:
  
    def enviar_dados_node(self, chunk, nome_arq, idx):
        chunk_base64 = chunk["data"]
        chunk = base64.b64decode(chunk_base64)
        caminho_arquivo = Path(f"{nome_arq}{idx}.bin")
        if caminho_arquivo.exists():
            return False
        try:

            # print(chunk)
            with open(caminho_arquivo, "wb") as f:
                f.write(chunk)

        except IOError as e:
            print(f"Ocorreu um erro de I/O (Entrada/Saída): {e}")
        return True
    
    def ler_dados_node(self,nome_arq, indice):

        caminho_deste_script = Path(__file__).resolve() 
        diretorio_do_script = caminho_deste_script.parent 
        # 1. Constrói o caminho completo para o arquivo
        nome_arquivo_alvo = f"{nome_arq}{indice}.bin"
        caminho_completo_alvo = diretorio_do_script / nome_arquivo_alvo

        # 2. Verifica se o arquivo existe
        if caminho_completo_alvo.is_file():
            try:
                # 3. Lê todo o conteúdo do arquivo em modo binário
                dados_em_bytes = caminho_completo_alvo.read_bytes()

                # print(f">>> Arquivo lido com sucesso! Retornando {len(dados_em_bytes)} bytes.")

                # 4. Retorna o conteúdo em bytes
                return dados_em_bytes
            except Exception as e:
                print(f"Erro ao ler o arquivo '{caminho_completo_alvo}': {e}")
                return None
        else:
            print("--- Arquivo não encontrado.")
            return None
    
    def remover_dados_node(self, nome_arq, indice):

        caminho_deste_script = Path(__file__).resolve()
        diretorio_do_script = caminho_deste_script.parent 
        # 1. Constrói o caminho completo para o arquivo alvo
        nome_arquivo_alvo = f"{nome_arq}{indice}.bin"
        caminho_completo_alvo = diretorio_do_script / nome_arquivo_alvo

        # print(f"Tentando remover o arquivo: {caminho_completo_alvo}")
        if caminho_completo_alvo.is_file():
            try:
                caminho_completo_alvo.unlink()
                # print(">>> Arquivo excluído com sucesso!")
            except Exception as e:
                print(f"Ocorreu um erro ao tentar excluir o arquivo: {e}")
        else:
            print("--- O arquivo não foi encontrado para ser excluído.")
            return False

        # Verificação final
        if not caminho_completo_alvo.exists():
            # print("Verificação confirmada: o arquivo não existe mais.")
            return True

    def limpar_pasta_de_dados(self, arquivos_para_ignorar):
        """
        Percorre uma pasta e exclui todos os arquivos, exceto os que estão
        na lista de ignorados.

        Retorna o número de arquivos que foram excluídos.
        """
        caminho_deste_script = Path(__file__).resolve() # .resolve() para garantir que seja absoluto
        # .parent pega o diretório "pai" do script (a pasta 'node1')
        diretorio_do_script = caminho_deste_script.parent 
        # 1. Constrói o caminho completo para o arquivo alvo

        print(f"\n--- Iniciando limpeza na pasta: '{diretorio_do_script}' ---")

        # Verifica se o caminho fornecido é realmente uma pasta que existe
        if not diretorio_do_script.is_dir():
            print(f"Erro: A pasta '{diretorio_do_script}' não existe ou não é um diretório.")
            return 0 # Retorna 0 arquivos excluídos

        arquivos_excluidos = 0
        # Itera sobre cada item (arquivo ou pasta) dentro da pasta_alvo
        for item in diretorio_do_script.iterdir():

            # Condição 1: O item é um arquivo?
            # Condição 2: O nome do item NÃO ESTÁ na lista de ignorados?
            if item.is_file() and item.name not in arquivos_para_ignorar:
                try:
                    # print(f"  - Excluindo arquivo de dados: {item.name}")
                    item.unlink() # Deleta o arquivo
                    arquivos_excluidos += 1
                except Exception as e:
                    print(f"  - ERRO ao excluir {item.name}: {e}")
            else:
                print(f"  - Ignorando: {item.name} (é uma pasta ou está na lista de exceções).")

        print(f"--- Limpeza concluída. {arquivos_excluidos} arquivo(s) foram removidos de '{diretorio_do_script}'. ---")
        return arquivos_excluidos
#Cria um daemon no servidor, que servirá como um "despachante" para lidar com 
#requisições que chegam dos clientes, e direcioná-las a um objeto remoto registrado.


# daemon para escutar requisições nesse IP
daemon = Pyro5.api.Daemon(host=ip_servidor)




# localiza o serviço de nomes no IP informado (esse ip deve ser aquele que identifica 
# a máquina em que está o name server)
ns = Pyro5.api.locate_ns(host=ip_name_server)




#Daemon registra a classe como remota, e quando uma invocação de um método da classe por um proxy do
#cliente chegar, o daemon se encarrega de criar a instância dessa classe no servidor, implementar o 
#método e retornar o resultado ao proxy do cliente. Quando o cliente se desconectar, o daemon remove
#a instância criada. Quando outro cliente invoca algum método da classe, ele cria outra instância.
#Quando passa-se uma classe como parâmetro, a uri que o método register retorna é uma referência para
#esse serviço do daemon de criar uma instância para conexão com um proxy e retornar. Quando passa-se um 
#obejto a uri retornada referencia o objeto em si. Então com essa uri, o proxy do cliente consegue se 
#comunicar com um objeto específico (objeto como parâmetro) ou um objeto instanciado pelo daemon (classe
#como parâmetro).
uri = daemon.register(sist_arq)


#proxy do servidor de nomes registra o objeto remoto no servidor de nomes pela uri do objeto,
#e o atribui um nome lógico "sistema_arquivos" pelo qual ele pode ser localizado.
ns.register("node1", uri)
print("uri do objeto remoto: ", uri)
uri1 = ns.lookup("monitor")

# Cria e inicia a thread de heartbeat

# 'daemon=True' garante que a thread feche quando o programa principal fechar
try:
    heartbeat_thread = threading.Thread(
        target=ping, 
        args=(uri1,), # Envia a cada 30 segundos
        daemon=True
    )
    heartbeat_thread.start()
except Pyro5.errors.NamingError as e:
    print(f"Erro: Não foi possível encontrar o servidor monitor no Name Server. {e}")


#O daemon entra em um loop para receber as requisições que chegam dos proxys do cliente. Por padrão, 
#o request loops é multithreading. Ou seja, para cada novo proxy do cliente conectado, cria-se uma
#nova thread no servidor.
daemon.requestLoop()