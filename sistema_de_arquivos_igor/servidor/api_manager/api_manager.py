import Pyro5.client
import Pyro5.api
import os
import base64
from itertools import islice
import hashlib
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict
import queue
import time
from pathlib import Path
import uuid

ip_servidor = '192.168.0.5'
ip_name_server = '192.168.0.5'

CHUNK_SIZE = 1024*1024

#Identifica a classe como remota
@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single") # Garante que só teremos UMA instância do Monitor
class sist_arq:
    def __init__(self):
        
        self.uri_ns = ip_name_server
        nameserver = Pyro5.api.locate_ns(host=self.uri_ns)

        self.nodes_que_ja_cairam = set()
        self.lock_estado_nodes = threading.Lock()

        self.monitoramento = False
        self.lock_monitoramento = threading.Lock()


        self.pasta_uploads_temp = Path("temp_uploads")
        self.pasta_uploads_temp.mkdir(exist_ok=True)

        self.proxy_file_map = None
        self.proxy_monitor = None
        try:
            self.uri_filemap = nameserver.lookup("sistema_arquivos_1")
            self.uri_monitor = nameserver.lookup("monitor") 
            # self.sistema_file_map=Pyro5.client.Proxy(self.uri_filemap)
            # self.sistema_monitor=Pyro5.client.Proxy(self.uri_monitor)
            print("Conectado com sucesso ao File Map e ao Monitor.")
        except Pyro5.errors.NamingError as e:
            print(f"Erro: Não foi possível encontrar um dos servidores de arquivos no Name Server. {e}")
        
        # --- Inicia a thread de monitoramento em segundo plano ---
        self.thread_monitoramento = threading.Thread(target=self._run_monitoramento, daemon=True)
        self.thread_monitoramento.start()
        print("Thread de monitoramento de status iniciada pelo API Manager.")

    def _run_monitoramento(self):

        while self.proxy_monitor is None:
            try:
                self.proxy_monitor = Pyro5.api.Proxy(self.uri_monitor)
                print(f"[Thread Monitor Cliente] Conexão com o Monitor estabelecida.")
            except Exception as e:
                print(f"[Thread Monitor Cliente] Falha ao conectar com o Monitor ({e}). Tentando novamente em 10s...")
                time.sleep(10)

        self.proxy_filemap = None
        while self.proxy_filemap is None:
            try:
                self.proxy_filemap = Pyro5.api.Proxy(self.uri_filemap)
                print(f"[Thread Monitor Cliente] Conexão com o Monitor estabelecida.")
            except Exception as e:
                print(f"[Thread Monitor Cliente] Falha ao conectar com o Monitor ({e}). Tentando novamente em 10s...")
                time.sleep(10)

        while True:
            try:
                # 1. Recebe o status atual de todos os nós do monitor
                status_atual_nodes = self.proxy_monitor.receber_status()

                for node_id, dados in status_atual_nodes.items():
                    status = dados.get('status')
                    
                    # 2. Lógica para quando um nó está INATIVO
                    if status == 'inativo':
                        # Verifica se é a PRIMEIRA VEZ que vemos este nó cair
                        if node_id not in self.nodes_que_ja_cairam:
                            print(f"MONITOR CLIENTE: Detectada nova falha no '{node_id}'!")
                            
                            # ---- INÍCIO DO MODO DE MANUTENÇÃO ----
                            print("MONITOR CLIENTE: MODO DE MANUTENÇÃO ATIVADO. Operações principais pausadas.")
                            with self.lock_monitoramento:
                                self.monitoramento = True # Trava o sistema
                            
                            self.nodes_que_ja_cairam.add(node_id)
                            self._reconfigurar_por_falha(node_id)
                            
                            with self.lock_monitoramento:
                                self.monitoramento = False # Trava o sistema
                            print("MONITOR CLIENTE: MODO DE MANUTENÇÃO DESATIVADO. Operações liberadas.")
                            # ----------------------------------------
                            
                    # 3. Lógica para quando um nó ATIVO volta
                    elif status == 'ativo':
                        # Se o nó estava na nossa lista de 'caídos', nós o removemos.
                        # Isso o "reseta", permitindo que uma futura queda seja tratada novamente.
                        if node_id in self.nodes_que_ja_cairam:
                            print(f"MONITOR CLIENTE: Nó '{node_id}' voltou a ficar ativo. Resetando flag de falha.")
                            with self.lock_monitoramento:
                                self.monitoramento = True # Trava o sistema
                            self.nodes_que_ja_cairam.remove(node_id)
                            ip_name_server = self.uri_ns
                            nameserver = Pyro5.api.locate_ns(host=ip_name_server)
                            node_uri = nameserver.lookup(f"node{node_id}")
                            self.proxy_node = Pyro5.api.Proxy(node_uri)
                            arquivo_nao_deletavel = f"node{node_id}.py"
                            self.proxy_node.limpar_pasta_de_dados(arquivo_nao_deletavel)
                            with self.lock_monitoramento:
                                self.monitoramento = False # Trava o sistema
                            print("MONITOR CLIENTE: MODO DE MANUTENÇÃO DESATIVADO. Operações liberadas.")
                            # ----------------------------------------

            except Exception as e:
                print(f"MONITOR CLIENTE: Erro ao obter status do servidor: {e}")
            
            # Espera 15 segundos para a próxima verificação
            time.sleep(10)
    

    def _reconfigurar_por_falha(self,node_id_falho):

        ip_name_server = self.uri_ns
        nameserver = Pyro5.api.locate_ns(host=ip_name_server)

        print(f"AÇÃO: Reconfigurando o sistema devido à falha do nó '{node_id_falho}'...")
        lista_nomes = self.proxy_filemap.listar_filemap()


        for nome_arq in lista_nomes:
            self.proxy_filemap.modificar_file_map(nome_arq, node_id_falho)
            status_dict1 = self.proxy_monitor.receber_status()
            for node_id, dados_do_node in status_dict1.items():
                status_atual = dados_do_node['status']
                print(f"Nó: {node_id}, Status: {status_atual}")
            try: 
                try:
                    arq_idxs, fator_replica = self.proxy_filemap.get_filemap(nome_arq)
                    todos_os_numeros_nodes = []
                    # print("Percorrendo e coletando todos os IDs dos nós...")
                    for _, lista_nodes in arq_idxs:
                        for node_id in lista_nodes:
                            # Adicione cada número à lista
                            todos_os_numeros_nodes.append(node_id)
                    # print(f"Lista com todos os nós (com repetições): {todos_os_numeros_nodes}")
                    nodes_unicos_set = set(todos_os_numeros_nodes)
                    # print(f"Conjunto de nós únicos (sem ordem garantida): {nodes_unicos_set}")
                    lista_final_ordenada = sorted(nodes_unicos_set)
                    print("Obtendo URIs dos nós de armazenamento...")
                    uris_dos_nodes1 = {
                        node_id: nameserver.lookup(f"node{node_id}")
                        for node_id in lista_final_ordenada
                    }
                    print("URIs obtidas:", uris_dos_nodes1)
                except Pyro5.errors.NamingError as e:
                    print(f"Erro fatal: Não foi possível encontrar um dos nós no Name Server. {e}")
                    return # Aborta a operação se não encontrar os nós
                print("Iniciando recebimento paralelo dos chunks...")
                trabalho_por_uri = defaultdict(list)
                if (arq_idxs):
                    for (idx_chunk, lista_nodes) in arq_idxs:
                        numero_node = lista_nodes[0]
                        # Determina para qual nó enviar
                        if numero_node in uris_dos_nodes1 and status_dict1[numero_node]['status'] == 'ativo':
                            uri_alvo = uris_dos_nodes1[numero_node]
                            tarefa = (nome_arq, idx_chunk)
                            trabalho_por_uri[uri_alvo].append(tarefa) # Funciona!
                        else:
                           print(f"Chunk {idx_chunk} não será recebido - nó {numero_node} inativo ou desconhecido.")
                    print("Distribuição do trabalho concluída. Preparando threads...")
                    #=========================================================================
                    # FASE 2: EXECUÇÃO (CONSUMIDORES)
                    # =========================================================================
                    threads = []
                    fila_de_resultados = queue.Queue()

                    # Itera sobre o dicionário de trabalho que acabamos de criar
                    for uri, lista_de_tarefas in trabalho_por_uri.items():
                        print(f"Criando thread para o destino {uri.location} com {len(lista_de_tarefas)} tarefas.")
                        thread = threading.Thread(
                            target=self._receber_do_node,
                            args=(uri, lista_de_tarefas, fila_de_resultados)
                        )
                        threads.append(thread)
                        thread.start() # Inicia a thread

                    # Espera todas as threads terminarem seu trabalho
                    print("\nThreads iniciadas. Aguardando a finalização de todos os recebimentos...")
                    for thread in threads:
                        thread.join()

                    print("\nRecebimento de todos os chunks concluído com sucesso!")
                    lista_fragmentos = []
                    while not fila_de_resultados.empty():
                        item = fila_de_resultados.get()
                        lista_fragmentos.append(item)
                        
                    # --- FASE 3: RECONSTRUÇÃO DO ARQUIVO (a mesma de antes) ---
                    if lista_fragmentos:
                        print(f"{len(lista_fragmentos)} fragmentos coletados. Reconstruindo o arquivo...")
                        
                        lista_fragmentos.sort(key=lambda item: item[0])
                        
                        nome_arquivo_reconstruido = f"{nome_arq}"
                        with open(nome_arquivo_reconstruido, "wb") as f_final:
                            for _, chunk_data in lista_fragmentos:
                                f_final.write(chunk_data)
                                
                        print(f"Arquivo '{nome_arquivo_reconstruido}' reconstruído com sucesso!")
                    else:
                        print("Nenhum fragmento foi coletado. Falha na reconstrução.")
                else:
                    raise FileNotFoundError(f"Arquivo '{nome_arq}' não encontrado no servidor.")
            except FileExistsError as e:
                print(f"Erro de Validação: {e}")
                raise
            except Exception as e:
                resultado = f"Erro: {e}"
                raise
            except IsADirectoryError as e:
                resultado = f"Erro: {e}"
                raise
            except FileNotFoundError as e:
                resultado = f"Erro: {e}"
                raise

            diretorio_do_script = Path(__file__).parent
            caminho_diretorio = diretorio_do_script / nome_arq
            try:
                # Verifica se o arquivo de origem existe
                if not os.path.exists(caminho_diretorio):
                    raise FileNotFoundError(f"Arquivo de origem '{nome_arq}' não encontrado no cliente.")
                # Verifica se eh um diretorio
                if not os.path.isfile(caminho_diretorio):
                    raise IsADirectoryError(f"'{nome_arq}' é um diretório. Envio de pastas não é suportado.")
                
                chunks = []
                dict_filemap = {}
                dict_indices = {}
                indices = []
                with open(caminho_diretorio, "rb") as f:
                    i=1
                    while True:
                        chunk = f.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        chunks.append((chunk))
                        indices.append(i)
                        i = i+1
                    dict_filemap[nome_arq] = (chunks)
                    dict_indices[nome_arq] = (indices)
                    print(f"qtd chunks: {len(chunks)}")
                    print(f"qtd indices: {len(indices)}")
                # resultado, fator_replica, lista_nodes_utilizados = self.proxy_filemap.inserir_filemap(dict_indices)
                arq_idxs, fator_replica = self.proxy_filemap.get_filemap(nome_arq)
                todos_os_numeros_nodes = []
                # print("Percorrendo e coletando todos os IDs dos nós...")
                for _, lista_nodes in arq_idxs:
                    for node_id in lista_nodes:
                        # Adicione cada número à lista
                        todos_os_numeros_nodes.append(node_id)
                # print(f"Lista com todos os nós (com repetições): {todos_os_numeros_nodes}")
                nodes_unicos_set = set(todos_os_numeros_nodes)
                # print(f"Conjunto de nós únicos (sem ordem garantida): {nodes_unicos_set}")
                lista_final_ordenada = sorted(nodes_unicos_set)

                status_dict = self.proxy_monitor.receber_status()
                for node_id, dados_do_node in status_dict.items():
                    status_atual = dados_do_node['status']
                    print(f"Nó: {node_id}, Status: {status_atual}")
                try:
                    print("Obtendo URIs dos nós de armazenamento...")
                    uris_dos_nodes = {
                        node_id: nameserver.lookup(f"node{node_id}")
                        for node_id in lista_final_ordenada
                    }
                    print("URIs obtidas:", uris_dos_nodes)
                except Pyro5.errors.NamingError as e:
                    print(f"Erro fatal: Não foi possível encontrar um dos nós no Name Server. {e}")
                    return # Aborta a operação se não encontrar os nós
                for i in range(fator_replica):
                    if i == 0:
                        print("Iniciando envio paralelo dos chunks primários")
                    else:
                        print("Iniciando envio paralelo das réplicas dos chunks")
                    trabalho_por_uri = defaultdict(list)
                    for (indice, lista_nodes), chunk in zip(arq_idxs, dict_filemap[nome_arq]):
                        numero_node = lista_nodes[i]
                        if numero_node in uris_dos_nodes and status_dict[numero_node]['status'] == 'ativo':
                            uri_alvo = uris_dos_nodes[numero_node]
                            tarefa = (chunk, nome_arq, indice)
                            trabalho_por_uri[uri_alvo].append(tarefa) # Funciona!
                        else:
                            print(f"Chunk {indice} não será enviado - nó {numero_node} inativo ou desconhecido.")
                    print("Distribuição do trabalho concluída. Preparando threads...")   
                    #=========================================================================
                    # FASE 2: EXECUÇÃO (CONSUMIDORES)
                    # =========================================================================
                    threads = []
                    # Itera sobre o dicionário
                    for uri, lista_de_tarefas in trabalho_por_uri.items():
                        print(f"Criando thread para o destino {uri.location} com {len(lista_de_tarefas)} tarefas.")
                        thread = threading.Thread(
                            target=self._enviar_trabalho_para_node,
                            args=(uri, lista_de_tarefas)
                        )
                        threads.append(thread)
                        thread.start() # Inicia a thread
                    # Espera todas as threads terminarem seu trabalho
                    print("\nThreads iniciadas. Aguardando a finalização de todos os envios...")
                    for thread in threads:
                        thread.join()
                    print("\nEnvio de todos os chunks concluído com sucesso!")
                caminho_diretorio.unlink()
            except FileExistsError as e:
                print(f"Erro de Validação: {e}")
                raise
            except Exception as e:
                print(f"Erro: {e}")
                raise
            except IsADirectoryError as e:
                print(f"Erro: {e}")
                raise
            except FileNotFoundError as e:
                print(f"Erro: {e}")
                raise


        



    def receber_chunk(self, nome_arq, chunk):
        "Recebe um chunk em base64 e o anexa a um arquivo temporário."
        try:
            chunk_base64 = chunk["data"]
            chunk = base64.b64decode(chunk_base64)
            caminho_temporario = self.pasta_uploads_temp / nome_arq
            
            with open(caminho_temporario, "ab") as f:
                f.write(chunk)
            return True
        except Exception as e:
            print(f"Erro ao receber chunk para '{nome_arq}': {e}")
            raise e
        
    def finalizar_upload(self, nome_arq):
            if self.monitoramento == True:
                raise Exception(f"Em manutenção")
            caminho_temporario = self.pasta_uploads_temp / nome_arq
            if not caminho_temporario.exists():
                raise FileNotFoundError(f"Upload de '{nome_arq}' finalizado, mas o arquivo temporário não foi encontrado.")

            print(f"Upload completo de '{nome_arq}' recebido. Disparando distribuição em segundo plano...")

            try:
                inicio_operacao = time.time() # Marca o tempo ANTES da operação
                resultado_distribuicao = self._inserir(nome_arq, str(caminho_temporario))
                fim_operacao = time.time()   # Marca o tempo DEPOIS da operação
                tempo_decorrido = fim_operacao - inicio_operacao
                print(f"Distribuição de '{nome_arq}' concluída com sucesso.")
                # Se _inserir terminar sem erros, retorna a mensagem de sucesso para o cliente.
                caminho_temporario.unlink()
                return resultado_distribuicao, tempo_decorrido

            except Exception as e:
               
                print(f"ERRO durante a distribuição síncrona de '{nome_arq}': {e}")
                raise
    def _inserir(self, nome_arq, caminho_arq_temp):
        if self.monitoramento == True:
            raise Exception(f"Em manutenção")
        if not self.uri_filemap or not self.uri_monitor:
            # Lançar o erro aqui, fora de um try/except local, permite que o Pyro o envie ao cliente.
            raise RuntimeError("Configuração inválida no servidor: URI do File Map ou monitor não foi localizada na inicialização.")
        try:
            nameserver = Pyro5.api.locate_ns(host=self.uri_ns)
            # A criação do proxy e a chamada são as operações que podem falhar por rede
            proxy_file_map = Pyro5.api.Proxy(self.uri_filemap)
            proxy_monitor = Pyro5.api.Proxy(self.uri_monitor)
            lista_nomes = proxy_file_map.listar_filemap()
            if nome_arq in lista_nomes:
                caminho_arq_temp_path = Path(caminho_arq_temp)
                caminho_arq_temp_path.unlink()
                raise FileExistsError(f"O arquivo '{nome_arq}' já existe no sistema de arquivos remoto.")
            try:
                # Verifica se o arquivo de origem existe
                if not os.path.isfile(caminho_arq_temp):
                    raise IsADirectoryError(f"'{nome_arq}' é um diretório. Envio de pastas não é suportado.")
                
                chunks = []
                dict_filemap = {}
                dict_indices = {}
                indices = []
                with open(caminho_arq_temp, "rb") as f:
                    i=1
                    while True:
                        chunk = f.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        chunks.append((chunk))
                        indices.append(i)
                        i = i+1
                    dict_filemap[nome_arq] = (chunks)
                    dict_indices[nome_arq] = (indices)
                    print(f"qtd chunks: {len(chunks)}")
                    print(f"qtd indices: {len(indices)}")  
                resultado, fator_replica, lista_nodes_utilizados = proxy_file_map.inserir_filemap(dict_indices)

                status_dict = proxy_monitor.receber_status()
                for node_id, dados_do_node in status_dict.items():
                    status_atual = dados_do_node['status']
                    print(f"Nó: {node_id}, Status: {status_atual}")
                try:
                    print("Obtendo URIs dos nós de armazenamento...")
                    uris_dos_nodes = {
                        node_id: nameserver.lookup(f"node{node_id}")
                        for node_id in lista_nodes_utilizados
                    }
                    print("URIs obtidas:", uris_dos_nodes) 
                except Pyro5.errors.NamingError as e:
                    print(f"Erro fatal: Não foi possível encontrar um dos nós no Name Server. {e}")
                    raise e
                
                for i in range(fator_replica):
                    if i == 0:
                        print("Iniciando envio paralelo dos chunks primários")
                    else:
                        print("Iniciando envio paralelo das réplicas dos chunks")
                    trabalho_por_uri = defaultdict(list)
                    for (indice, lista_nodes), chunk in zip(resultado[nome_arq], dict_filemap[nome_arq]):
                        numero_node = lista_nodes[i]
                        if numero_node in uris_dos_nodes and status_dict[numero_node]['status'] == 'ativo':
                            uri_alvo = uris_dos_nodes[numero_node]
                            tarefa = (chunk, nome_arq, indice)
                            trabalho_por_uri[uri_alvo].append(tarefa) # Funciona!
                        else:
                            print(f"Chunk {indice} não será enviado - nó {numero_node} inativo ou desconhecido.")  
                    print("Distribuição do trabalho concluída. Preparando threads...")     
                    #=========================================================================
                    # FASE 2: EXECUÇÃO (CONSUMIDORES)
                    # =========================================================================
                    threads = []   
                    # Itera sobre o dicionário de trabalho que acabamos de criar
                    for uri, lista_de_tarefas in trabalho_por_uri.items():
                        print(f"Criando thread para o destino {uri.location} com {len(lista_de_tarefas)} tarefas.")
                        thread = threading.Thread(
                            target=self._enviar_trabalho_para_node,
                            args=(uri, lista_de_tarefas)
                        )
                        threads.append(thread)
                        thread.start() # Inicia a thread   
                    # Espera todas as threads terminarem seu trabalho
                    print("\nThreads iniciadas. Aguardando a finalização de todos os envios...")
                    for thread in threads:
                        thread.join()  
                return True
            except Exception as e:
                print(f"Erro: {e}")
                raise(f"{e}")
            except IsADirectoryError as e:
                print(f"Erro: {e}")
                raise IsADirectoryError(f"'{nome_arq}' é um diretório. Envio de pastas não é suportado.")
            
        except Pyro5.errors.CommunicationError as e:
            print(f"ERRO no método 'listar' ao contatar o filemap: {e}")
            # Lança um novo erro para informar o cliente da falha de comunicação interna
            raise RuntimeError(f"Falha de comunicação interna no servidor ao contatar o File Map: {e}")
        except FileExistsError as e:
            print(e)
            raise FileExistsError(f"O arquivo '{nome_arq}' já existe no sistema de arquivos remoto.")
    def ler(self, nome_arq):
        if self.monitoramento == True:
            raise Exception(f"Em manutenção")      
        if not self.uri_filemap or not self.uri_monitor:
            # Lançar o erro aqui, fora de um try/except local, permite que o Pyro o envie ao cliente.
            raise RuntimeError("Configuração inválida no servidor: URI do File Map ou monitor não foi localizada na inicialização.")
        try:
            nameserver = Pyro5.api.locate_ns(host=self.uri_ns)
            # A criação do proxy e a chamada são as operações que podem falhar por rede
            proxy_file_map = Pyro5.api.Proxy(self.uri_filemap)
            proxy_monitor = Pyro5.api.Proxy(self.uri_monitor)
            lista_nomes = proxy_file_map.listar_filemap()
            if nome_arq not in lista_nomes:
                raise FileExistsError(f"O arquivo '{nome_arq}' não existe no servidor")
            status_dict1 = proxy_monitor.receber_status()
            for node_id, dados_do_node in status_dict1.items():
                status_atual = dados_do_node['status']
                print(f"Nó: {node_id}, Status: {status_atual}")
            try: 
                try:
                    arq_idxs, fator_replica = proxy_file_map.get_filemap(nome_arq)
                    todos_os_numeros_nodes = []
                    # print("Percorrendo e coletando todos os IDs dos nós...")
                    for _, lista_nodes in arq_idxs:
                        for node_id in lista_nodes:
                            # Passo 3: Adicione cada número à lista
                            todos_os_numeros_nodes.append(node_id)

                    # print(f"Lista com todos os nós (com repetições): {todos_os_numeros_nodes}")

                    nodes_unicos_set = set(todos_os_numeros_nodes)
                    # print(f"Conjunto de nós únicos (sem ordem garantida): {nodes_unicos_set}")
                    lista_final_ordenada = sorted(nodes_unicos_set)

                    print("Obtendo URIs dos nós de armazenamento...")
                    uris_dos_nodes1 = {
                        node_id: nameserver.lookup(f"node{node_id}")
                        for node_id in lista_final_ordenada
                    }
                    print("URIs obtidas:", uris_dos_nodes1)

                except Pyro5.errors.NamingError as e:
                    print(f"Erro fatal: Não foi possível encontrar um dos nós no Name Server. {e}")
                    raise 
                
                print("Iniciando recebimento paralelo dos chunks...")
                trabalho_por_uri = defaultdict(list)
                if (arq_idxs):
                    for (idx_chunk, lista_nodes) in arq_idxs:
                        numero_node = lista_nodes[0]
                        # Determina para qual nó enviar
                        if numero_node in uris_dos_nodes1 and status_dict1[numero_node]['status'] == 'ativo':
                            uri_alvo = uris_dos_nodes1[numero_node]
                            tarefa = (nome_arq, idx_chunk)
                            trabalho_por_uri[uri_alvo].append(tarefa) # Funciona!
                        else:
                           print(f"Chunk {idx_chunk} não será recebido - nó {numero_node} inativo ou desconhecido.")
                    print("Distribuição do trabalho concluída. Preparando threads...")
                    #=========================================================================
                    # FASE 2: EXECUÇÃO (CONSUMIDORES)
                    # =========================================================================
                    threads = []
                    fila_de_resultados = queue.Queue()

                    # Itera sobre o dicionário de trabalho que acabamos de criar
                    for uri, lista_de_tarefas in trabalho_por_uri.items():
                        print(f"Criando thread para o destino {uri.location} com {len(lista_de_tarefas)} tarefas.")
                        thread = threading.Thread(
                            target=self._receber_do_node,
                            args=(uri, lista_de_tarefas, fila_de_resultados)
                        )
                        threads.append(thread)
                        thread.start() # Inicia a thread

                    # Espera todas as threads terminarem seu trabalho
                    print("\nThreads iniciadas. Aguardando a finalização de todos os recebimentos...")
                    for thread in threads:
                        thread.join()

                    print("\nRecebimento de todos os chunks concluído com sucesso!")

                        
                    # --- FASE 3: RECONSTRUÇÃO DO ARQUIVO (a mesma de antes) ---
                    try:
                        lista_fragmentos = []
                        while not fila_de_resultados.empty():
                            item = fila_de_resultados.get()
                            lista_fragmentos.append(item)
                        print(f"{len(lista_fragmentos)} fragmentos coletados. Reconstruindo o arquivo...")
                        
                        lista_fragmentos.sort(key=lambda item: item[0])
                        nome_temp_unico = f"temp_reconstrucao_{nome_arq}_{uuid.uuid4().hex}.bin"
                        caminho_arquivo_reconstruido = self.pasta_uploads_temp / nome_temp_unico

                        # nome_arquivo_reconstruido = f"{nome_arq}"
                        with open(caminho_arquivo_reconstruido, "wb") as f_final:
                            for _, chunk_data in lista_fragmentos:
                                f_final.write(chunk_data)
                        
                        caminho_arquivo_reconstruido = Path(caminho_arquivo_reconstruido)
                        print(f"Lendo o arquivo '{caminho_arquivo_reconstruido}' para enviar ao cliente...")

                        # 1. Lê o arquivo inteiro do disco para a memória
                        dados_completos_em_bytes = caminho_arquivo_reconstruido.read_bytes()

                        # 2. (Opcional, mas recomendado) Apaga a cópia temporária do servidor
                        
                        caminho_arquivo_reconstruido.unlink()

                        # 3. Retorna o objeto de bytes. O Pyro cuidará do resto.
                        print(f"Enviando {len(dados_completos_em_bytes)} bytes para o cliente...")
                        return dados_completos_em_bytes

                    except Exception as e:
                        raise RuntimeError(f"Falha ao ler o arquivo reconstruído no servidor para envio: {e}")
                else:
                    raise FileNotFoundError(f"Arquivo '{nome_arq}' não encontrado nos metadados.")

            except Exception as e:
                print(f"Erro: {e}")
                raise
            except IsADirectoryError as e:
                print(f"Erro: {e}")
                raise
            except FileNotFoundError as e:
                print(f"Erro: {e}")
                raise

        except Pyro5.errors.CommunicationError as e:
            print(f"ERRO no método 'listar' ao contatar o filemap: {e}")
            # Lança um novo erro para informar o cliente da falha de comunicação interna
            raise RuntimeError(f"Falha de comunicação interna no servidor ao contatar o File Map: {e}")
        except FileExistsError as e:
            print(e)
            raise 

        
    def remover(self, nome_arq):
        if self.monitoramento == True:
            raise Exception(f"Em manutenção")
        if not self.uri_filemap or not self.uri_monitor:
            # Lançar o erro aqui, fora de um try/except local, permite que o Pyro o envie ao cliente.
            raise RuntimeError("Configuração inválida no servidor: URI do File Map ou monitor não foi localizada na inicialização.")
        try:
            nameserver = Pyro5.api.locate_ns(host=self.uri_ns)
            # A criação do proxy e a chamada são as operações que podem falhar por rede
            proxy_file_map = Pyro5.api.Proxy(self.uri_filemap)
            proxy_monitor = Pyro5.api.Proxy(self.uri_monitor)
            lista_nomes = proxy_file_map.listar_filemap()
            if nome_arq not in lista_nomes:
                raise FileExistsError(f"O arquivo '{nome_arq}' não existe no servidor")

            status_dict1 = proxy_monitor.receber_status()
            for node_id, dados_do_node in status_dict1.items():
                status_atual = dados_do_node['status']
                print(f"Nó: {node_id}, Status: {status_atual}")

            try: 
                try:
                    arq_idxs, fator_replica = proxy_file_map.get_filemap(nome_arq)
                    todos_os_numeros_nodes = []
                    # print("Percorrendo e coletando todos os IDs dos nós...")
                    for _, lista_nodes in arq_idxs:
                        for node_id in lista_nodes:
                            # Passo 3: Adicione cada número à lista
                            todos_os_numeros_nodes.append(node_id)

                    # print(f"Lista com todos os nós (com repetições): {todos_os_numeros_nodes}")

                    nodes_unicos_set = set(todos_os_numeros_nodes)
                    # print(f"Conjunto de nós únicos (sem ordem garantida): {nodes_unicos_set}")
                    lista_final_ordenada = sorted(nodes_unicos_set)

                    print("Obtendo URIs dos nós de armazenamento...")
                    uris_dos_nodes2 = {
                        node_id: nameserver.lookup(f"node{node_id}")
                        for node_id in lista_final_ordenada
                    }
                    print("URIs obtidas:", uris_dos_nodes2)

                except Pyro5.errors.NamingError as e:
                    print(f"Erro fatal: Não foi possível encontrar um dos nós no Name Server. {e}")
                    raise 
                arq_idxs, fator_replica = proxy_file_map.get_filemap(nome_arq)
                # lista_lista_recebimento = []
                for i in range(fator_replica):
                    if i == 0:    
                        print("Iniciando remoção paralela dos chunks primários...")
                    else:
                        print("iniciando remoção das réplicas dos chunks")
                    trabalho_por_uri = defaultdict(list)
                    if (arq_idxs):
                        for (idx_chunk, lista_nodes) in arq_idxs:
                            numero_node = lista_nodes[i]
                            # Determina para qual nó enviar
                            if numero_node in uris_dos_nodes2 and status_dict1[numero_node]['status'] == 'ativo':
                                uri_alvo = uris_dos_nodes2[numero_node]
                                tarefa = (nome_arq, idx_chunk)
                                trabalho_por_uri[uri_alvo].append(tarefa) # Funciona!
                            else:
                                print(f"Chunk {idx_chunk} não será recebido - nó {numero_node} inativo ou desconhecido.")

                        print("Distribuição do trabalho concluída. Preparando threads...")
                        #=========================================================================
                        # FASE 2: EXECUÇÃO (CONSUMIDORES)
                        # =========================================================================
                        threads = []
                        fila_de_resultados = queue.Queue()
                        # Itera sobre o dicionário de trabalho que acabamos de criar
                        for uri, lista_de_tarefas in trabalho_por_uri.items():
                            print(f"Criando thread para o destino {uri.location} com {len(lista_de_tarefas)} tarefas.")
                            thread = threading.Thread(
                                target=self._remover_do_node,
                                args=(uri, lista_de_tarefas, fila_de_resultados)
                            )
                            threads.append(thread)
                            thread.start() # Inicia a thread

                        # Espera todas as threads terminarem seu trabalho
                        print("\nThreads iniciadas. Aguardando a finalização de todos os envios...")
                        for thread in threads:
                            thread.join()

                        print("\nThreads de remoção concluídas.")

                        # lista_lista_recebimento.append(lista_recebimento)

                    else:
                        raise FileNotFoundError(f"Arquivo '{nome_arq}' não encontrado nos metadados.")

                if proxy_file_map.remover_filemap(nome_arq):
                    return True

            except Exception as e:
                print(f"Erro: {e}")
                raise
            except IsADirectoryError as e:
                print(f"Erro: {e}")
                raise
            except FileNotFoundError as e:
                print(f"Erro: {e}")
                raise

        except Pyro5.errors.CommunicationError as e:
            print(f"ERRO no método 'listar' ao contatar o filemap: {e}")
            # Lança um novo erro para informar o cliente da falha de comunicação interna
            raise RuntimeError(f"Falha de comunicação interna no servidor ao contatar o File Map: {e}")
        except FileExistsError as e:
            print(e)
            raise

    def listar(self):
        if self.monitoramento == True:
            raise Exception(f"Em manutenção")
        if not self.uri_filemap:
            # Lançar o erro aqui, fora de um try/except local, permite que o Pyro o envie ao cliente.
            raise RuntimeError("Configuração inválida no servidor: URI do File Map não foi localizada na inicialização.")

        # 2. O bloco try/except agora foca em erros de COMUNICAÇÃO, que podem acontecer durante a chamada
        try:
            # A criação do proxy e a chamada são as operações que podem falhar por rede
            proxy_file_map = Pyro5.api.Proxy(self.uri_filemap)
            lista_nomes = proxy_file_map.listar_filemap()
            linhas_de_texto = []
            for nome in lista_nomes:
                linhas_de_texto.append(nome)
            return "\n".join(linhas_de_texto)
        except Pyro5.errors.CommunicationError as e:
            print(f"ERRO no método 'listar' ao contatar o filemap: {e}")
            # Lança um novo erro para informar o cliente da falha de comunicação interna
            raise RuntimeError(f"Falha de comunicação interna no servidor ao contatar o File Map: {e}")
        
    def _verificar_nodes_manutencao(self):
        return
    

    def _enviar_trabalho_para_node(self,node_uri, lista_de_tarefas):
        try:
            # Cria a conexão persistente (proxy) UMA ÚNICA VEZ
            proxy_node = Pyro5.api.Proxy(node_uri)
            print(f"THREAD para {node_uri.location}: Conexão estabelecida, iniciando envio de {len(lista_de_tarefas)} chunks.")
    
            # Percorre a lista de trabalho que recebeu
            for tarefa in lista_de_tarefas:
                # Desempacota a tarefa
                chunk, nome_arq, indice = tarefa
                # Codifica e envia os dados usando a mesma conexão
                proxy_node.enviar_dados_node(chunk, nome_arq, indice)
                # print(f"THREAD para {node_uri.location}: Chunk {indice} enviado.")
                
            print(f"THREAD para {node_uri.location}: Trabalho concluído!")
    
        except Exception as e:
            print(f"THREAD para {node_uri.location}: Erro fatal na thread - {e}")
    
    def _receber_do_node(self,node_uri, lista_de_tarefas, fila_de_resultados):
        try:
            proxy_node = Pyro5.api.Proxy(node_uri)
            print(f"THREAD para {node_uri.location}: Conexão estabelecida.")
    
            for tarefa in lista_de_tarefas:
                # Desempacota a tarefa
                nome_arq, idx_chunk = tarefa
                
                # Faz a chamada remota
                chunk = proxy_node.ler_dados_node(nome_arq, idx_chunk)
                chunk_base64 = chunk["data"]
                chunk = base64.b64decode(chunk_base64)
                    
                # EM VEZ DE 'RETURN', COLOCA O RESULTADO NA FILA
                fila_de_resultados.put((idx_chunk, chunk))
                # print(f"THREAD para {node_uri.location}: Chunk {idx_chunk} colocado na fila de resultados.")
            
            print(f"THREAD para {node_uri.location}: Todas as tarefas concluídas.")
    
        except Exception as e:
            print(f"THREAD para {node_uri.location}: Erro fatal na thread - {e}")
    
    
    def _remover_do_node(self,node_uri, lista_de_tarefas, fila_de_resultados):
        try:
            # Cria a conexão persistente (proxy) UMA ÚNICA VEZ
            proxy_node = Pyro5.api.Proxy(node_uri)
            print(f"THREAD para {node_uri.location}: Conexão estabelecida, iniciando envio de {len(lista_de_tarefas)} chunks.")
    
            # Percorre a lista de trabalho que recebeu
            for tarefa in lista_de_tarefas:
                # Desempacota a tarefa
                nome_arq, indice = tarefa
    
                result = proxy_node.remover_dados_node(nome_arq, indice)
                # print(f"THREAD para {node_uri.location}: Chunk {indice} removido.")
                fila_de_resultados.put((indice, result))
                
            print(f"THREAD para {node_uri.location}: Trabalho concluído!")
    
        except Exception as e:
            print(f"THREAD para {node_uri.location}: Erro fatal na thread - {e}")


#Cria um daemon no servidor, que servirá como um "despachante" para lidar com 
#requisições que chegam dos clientes, e direcioná-las a um objeto remoto registrado.

# endereço IP do servidor, substituir pelo ip da máquina que rodará o servidor


# daemon para escutar requisições nesse IP
daemon = Pyro5.api.Daemon(host=ip_servidor)

#Endereço ip do name_server 


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
ns.register("api_manager", uri)
print("uri do objeto remoto: ", uri)

#O daemon entra em um loop para receber as requisições que chegam dos proxys do cliente. Por padrão, 
#o request loops é multithreading. Ou seja, para cada novo proxy do cliente conectado, cria-se uma
#nova thread no servidor.
daemon.requestLoop()




