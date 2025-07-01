import Pyro5.server
import Pyro5.core
import os
import shutil
import Pyro5.api
import base64
import threading
from pathlib import Path
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


#Identifica a classe como remota
@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single") # Garante que só teremos UMA instância do Monitor
class sist_arq:
    def __init__(self):
        self.status_dos_nodes = {}

        self.lock_do_status = threading.Lock()
        print("Instância única do Monitor criada com dicionário compartilhado e lock.")

        try:
            ns = Pyro5.api.locate_ns(host=ip_name_server)
            self.uri_file_map = ns.lookup("sistema_arquivos_1") # O nome que você registrou para o file_map
        except Pyro5.errors.NamingError:
            print("AVISO: Serviço de File Map não encontrado. Notificações não funcionarão.")
        
        self.verificador_thread = threading.Thread(target=self._verificar_nodes_inativos, daemon=True)
        self.verificador_thread.start()
        print("Thread verificadora de status iniciada.")
    def enviar_ping(self, node_id):

        with self.lock_do_status:
            print(f"Lock adquirido pela thread do nó '{node_id}'. Acessando o dicionário...")
            
            # --- Início da Seção Crítica (código protegido) ---
            
            if node_id not in self.status_dos_nodes or self.status_dos_nodes[node_id]['status'] == 'inativo':
                print(f"  -> Primeiro contato de '{node_id}', ou recuperando atividade. Registrando.")
                try:
                    proxy_file_map = Pyro5.api.Proxy(self.uri_file_map)
                    print(f"Monitor conectado com sucesso ao File Map para notificar registro de node {node_id}")
                    # Chama o método no servidor de File Map
                    proxy_file_map.registrar_node_ativo(node_id)
                    print(f"node {node_id} registrado no file_map")
                except Exception as e:
                    print(f"ERRO ao notificar o File Map sobre o nó {node_id}: {e}")

            # Atualiza o dicionário compartilhado
            self.status_dos_nodes[node_id] = {
                "last_seen": time.time(),
                "status": "ativo"
            }
            print(self.status_dos_nodes)
            # --- Fim da Seção Crítica ---

        # O lock é automaticamente liberado aqui, ao sair do bloco 'with'.
        print(f"Lock liberado pela thread do nó '{node_id}'.")
        return True        
        
    def _verificar_nodes_inativos(self):
        """[RODA EM BACKGROUND] Verifica periodicamente por nós silenciosos."""
        TEMPO_LIMITE_SEGUNDOS = 20
        while True:
            time.sleep(15)
            agora = time.time()
            print(f"\n({time.ctime()}) --- Executando verificação de nós inativos ---")
            
            with self.lock_do_status:
                for node_id in list(self.status_dos_nodes.keys()):
                    dados_node = self.status_dos_nodes[node_id]
                    if dados_node['status'] == 'ativo':
                        tempo_desde_ultimo_sinal = agora - dados_node['last_seen']
                        if tempo_desde_ultimo_sinal > TEMPO_LIMITE_SEGUNDOS:
                            print(f"!!! NÓ '{node_id}' ficou INATIVO (último sinal há {tempo_desde_ultimo_sinal:.0f}s).")
                            self.status_dos_nodes[node_id]['status'] = 'inativo'
                    if dados_node['status'] == 'inativo':
                        try:
                            proxy_file_map = Pyro5.api.Proxy(self.uri_file_map)
                            print(f"Monitor conectado com sucesso ao File Map para notificar exclusão de node {node_id}")
                            # Chama o método no servidor de File Map
                            if proxy_file_map.excluir_node_ativo(node_id):
                                print(f"node {node_id} exluido de file_map")
                            else:
                                print(f"Erro ao notificar file map sobre exclusão do node {node_id}")
                        except Exception as e:
                            print(f"ERRO ao notificar o File Map sobre o nó {node_id}: {e}")
                        
            print("--- Verificação concluída ---")
            print(self.status_dos_nodes)
    
    def receber_status(self):
        return self.status_dos_nodes
    

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
ns.register("monitor", uri)
print("uri do objeto remoto: ", uri)

#O daemon entra em um loop para receber as requisições que chegam dos proxys do cliente. Por padrão, 
#o request loops é multithreading. Ou seja, para cada novo proxy do cliente conectado, cria-se uma
#nova thread no servidor.
daemon.requestLoop()