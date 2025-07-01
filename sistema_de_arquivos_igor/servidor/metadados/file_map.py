import Pyro5.server
import Pyro5.core
import os
import shutil
import Pyro5.api
import base64
import threading
from pathlib import Path
import random
import ast # Módulo para conversão segura de string para lista/dicionário
import shutil
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
@Pyro5.server.expose
@Pyro5.api.behavior(instance_mode="single") # Garante que só teremos UMA instância do Filemap
class sist_arq:
       
       def __init__(self):
            print(">>> Instância única de 'filemap' criada no servidor.")
            # 'self.fator_replica' agora é um atributo do objeto, acessível por todos os métodos.
            self.fator_replica = 2
            self.nodes_disponiveis = []

            self.lock_dos_nodes = threading.Lock()
            self.verificador_thread = threading.Thread(target=self._printar_lista_nodes, daemon=True)
            self.verificador_thread.start()
            self.lock_modificar_mapa = threading.Lock()

       def listar_filemap(self):
            caminho_deste_script = Path(__file__).resolve() # .resolve() para garantir que seja absoluto
            # .parent pega o diretório "pai" do script (a pasta 'metadados')
            diretorio_do_script = caminho_deste_script.parent 
            pasta_base = diretorio_do_script
            lista_nomes = []
            for caminho_subpasta in pasta_base.iterdir():
                if caminho_subpasta.is_dir():
                    lista_nomes.append(caminho_subpasta.name)

            return lista_nomes
           
       def inserir_filemap(self, dict):
            dict_return = {}
            indices = []
            for chave in dict.keys():
                print(chave)
                # Adquire o lock para ler a lista de nós de forma segura
                with self.lock_dos_nodes:
                    # Faz uma cópia da lista de nós. A partir daqui, trabalharemos com a cópia.
                    nodes_para_esta_operacao = self.nodes_disponiveis.copy()


                nome_da_pasta = chave

                # Cria um objeto de caminho para a nova pasta
                pasta_principal = Path(nome_da_pasta)

                pasta_principal.mkdir(exist_ok=True)
                print(f"Pasta '{pasta_principal}' foi criada ou já existia.")

                tamanho_lista_nodes = len(nodes_para_esta_operacao)
                contador_de_chunks = 0 # Contador que vai de 0, 1, 2, 3, ...
                pos_inicial = nodes_para_esta_operacao[0]
                lista_lista_nodes = []
                for indice in dict[chave]:
                    indice_ciclico = contador_de_chunks % tamanho_lista_nodes
                    pos_inicial = nodes_para_esta_operacao[indice_ciclico]
                    indices.append(indice)
                    # print(f"indice: {indice}")
                    numeros_filtrados = [numero for numero in nodes_para_esta_operacao if numero != pos_inicial]
                    # print(f"numeros filtrados: {numeros_filtrados}")
                    random.shuffle(numeros_filtrados)
                    # print(f"numeros filtrados randomizados: {numeros_filtrados}")
                    lista_nodes = []
                    lista_nodes.append(pos_inicial)
                    lista_nodes.extend(numeros_filtrados)
                    lista_lista_nodes.append(lista_nodes)

                    caminho_subpasta = pasta_principal / f"{indice}"
                    caminho_subpasta.mkdir(exist_ok=True)
                    # print(f"  - Subpasta '{caminho_subpasta}' pronta.")
                    caminho_arquivo_txt = caminho_subpasta / "info.txt"
                    conteudo_arquivo = f"{(lista_nodes)}"
                    caminho_arquivo_txt.write_text(conteudo_arquivo, encoding='utf-8')
                    # print(f"    - Arquivo '{caminho_arquivo_txt}' criado com sucesso.")
                    contador_de_chunks = contador_de_chunks + 1
                lista_return = []
                for idx, listaDenodes in zip(indices, lista_lista_nodes):
                     lista_return.append((idx, listaDenodes[:self.fator_replica]))
                dict_return[chave] = lista_return
            return (dict_return, self.fator_replica, nodes_para_esta_operacao)
       
       def get_filemap(self, nome_arq):
            caminho_deste_script = Path(__file__).resolve() # .resolve() para garantir que seja absoluto
            # .parent pega o diretório "pai" do script (a pasta 'metadados')
            diretorio_do_script = caminho_deste_script.parent
            pasta_base = diretorio_do_script
            print(f"Buscando filemap para o arquivo: {nome_arq}")
            pasta_arquivo = pasta_base / nome_arq
            if not pasta_arquivo.is_dir():
                print(f"Arquivo '{nome_arq}' não encontrado nos metadados.")
                return False
            lista_de_retorno = []
        
            subpastas_numericas = [
            subpasta for subpasta in pasta_arquivo.iterdir() 
            if subpasta.is_dir() and subpasta.name.isdigit()
            ]
            subpastas_ordenadas = sorted(subpastas_numericas, key=lambda p: int(p.name))
            # Percorrer as subpastas numeradas dentro da pasta do arquivo
            for caminho_subpasta in subpastas_ordenadas:
                # Garante que estamos olhando para um diretório e que seu nome é um número
                if caminho_subpasta.is_dir() and caminho_subpasta.name.isdigit():

                    indice_chunk = int(caminho_subpasta.name)
                    caminho_info_txt = caminho_subpasta / "info.txt"

                # Ler o info.txt
                if caminho_info_txt.exists():
                    try:
                        conteudo_str = caminho_info_txt.read_text(encoding='utf-8')
                        
                        # Converter a string '[1, 2]' para a lista [1, 2]
                        lista_nodes = ast.literal_eval(conteudo_str)
                        
                        lista_de_retorno.append((indice_chunk, lista_nodes[:self.fator_replica]))
                        
                    except (ValueError, SyntaxError) as e:
                        print(f"Aviso: Formato inválido no arquivo info.txt do chunk {indice_chunk}: {e}")
            lista_de_retorno.sort(key=lambda item: item[0])
            return (lista_de_retorno, self.fator_replica)
       def remover_filemap(self, nome_arq):
            caminho_deste_script = Path(__file__).resolve() # .resolve() para garantir que seja absoluto
            # .parent pega o diretório "pai" do script (a pasta 'metadados')
            diretorio_do_script = caminho_deste_script.parent 
            pasta_base = diretorio_do_script
            print(f"Buscando filemap para o arquivo: {nome_arq}")
            pasta_arquivo = pasta_base / nome_arq  

            print(f"Tentando remover a árvore de diretórios: {pasta_arquivo}")
            # Verifica se o caminho existe e se é de fato uma pasta
            if pasta_arquivo.is_dir():
                try:
                    # CORREÇÃO: shutil.rmtree() apaga a pasta e todo o seu conteúdo de uma só vez.
                    shutil.rmtree(pasta_arquivo)

                    print(f">>> Pasta de metadados '{nome_arq}' e todo o seu conteúdo foram excluídos.")
                    return True
                except Exception as e:
                    print(f"Ocorreu um erro ao tentar excluir a árvore de diretórios: {e}")
                    return False
            else:
                print(f"--- Metadados para '{nome_arq}' não encontrados ou não é uma pasta.")
                return False
            
        
       def _printar_lista_nodes(self):
            while True:
                time.sleep(15)
                print(self.nodes_disponiveis)
            
        
       def registrar_node_ativo(self, node_id):
            with self.lock_dos_nodes:
                self.nodes_disponiveis.append(node_id)
                self.nodes_disponiveis.sort()
                print(f"lista de nodes ativos: {self.nodes_disponiveis}")
            return True
       
       def excluir_node_ativo(self, node_id):
            if node_id in self.nodes_disponiveis:
                with self.lock_dos_nodes:
                    self.nodes_disponiveis.remove(node_id)
                    self.nodes_disponiveis.sort()
                    print(f"lista de nodes ativos: {self.nodes_disponiveis}")
            return True
       
       def modificar_file_map(self, nome_arq, node_id_inativo):
        """
        Percorre os metadados de um arquivo e remove um nó inativo de todas as listas de nós.
        """
        # Adquire o lock para garantir que a operação seja atômica.
        # Isso impede que duas threads tentem modificar o mesmo filemap ao mesmo tempo.
        with self.lock_modificar_mapa:
            print(f"MODIFICANDO FILEMAP: Iniciando remoção do nó {node_id_inativo} do arquivo '{nome_arq}'.")
            
            caminho_deste_script = Path(__file__).resolve() 
            # .parent pega o diretório "pai" do script (a pasta 'metadados')
            diretorio_do_script = caminho_deste_script.parent
            pasta_base = diretorio_do_script
            print(f"Buscando filemap para o arquivo: {nome_arq}")
            pasta_arquivo = pasta_base / nome_arq
            if not pasta_arquivo.is_dir():
                print(f"Arquivo '{nome_arq}' não encontrado nos metadados.")
                return False


            # Percorre as subpastas (1, 2, 3...)
            for caminho_subpasta in pasta_arquivo.iterdir():
                # Garante que estamos olhando para uma pasta de chunk (nome é um número)
                if not (caminho_subpasta.is_dir() and caminho_subpasta.name.isdigit()):
                    continue # Pula se não for (ex: outro arquivo ou pasta)

                caminho_info_txt = caminho_subpasta / "info.txt"
                
                # Lê o info.txt
                if caminho_info_txt.is_file():
                    try:
                        conteudo_str = caminho_info_txt.read_text('utf-8')
                        
                        # Converte a string '[1, 4]' para a lista [1, 4]
                        lista_nodes_antiga = ast.literal_eval(conteudo_str)
                        
                        # Cria a nova lista sem o nó inativo 
                        lista_nodes_nova = [node for node in lista_nodes_antiga if node != node_id_inativo]
                        
                        # Converte a nova lista de volta para string
                        conteudo_novo = str(lista_nodes_nova)
                        
                        # Sobrescreve o arquivo com o novo conteúdo
                        caminho_info_txt.write_text(conteudo_novo, 'utf-8')
                        
                        print(f"  - Chunk {caminho_subpasta.name}: Nós atualizados de {lista_nodes_antiga} para {lista_nodes_nova}")

                    except Exception as e:
                        print(f"  - Erro ao processar o chunk {caminho_subpasta.name}: {e}")
            
            print(f"Modificação do filemap para '{nome_arq}' concluída.")
            return True # Retorna True indicando sucesso

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
ns.register("sistema_arquivos_1", uri)
print("uri do objeto remoto: ", uri)

#O daemon entra em um loop para receber as requisições que chegam dos proxys do cliente. Por padrão, 
#o request loops é multithreading. Ou seja, para cada novo proxy do cliente conectado, cria-se uma
#nova thread no servidor.
daemon.requestLoop()