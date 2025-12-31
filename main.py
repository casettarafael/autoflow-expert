import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os
import re

# Lista de nichos para varrer a cidade (Adicione mais se quiser)
NICHOS_ALVO = [
    "Restaurantes", "Oficinas Mecânicas", "Salão de Beleza", "Dentistas", 
    "Advogados", "Pet Shop", "Academias", "Imobiliárias", "Contabilidade", 
    "Clínicas de Estética", "Lojas de Roupa", "Barbearias", "Pizzarias", 
    "Farmácias", "Floriculturas", "Materiais de Construção", "Auto Peças",
    "Supermercados", "Padarias", "Veterinários", "Depósitos de Construção",
    "Escolas", "Lojas de Informática", "Concessionárias", "Hotéis", 
    "Clínicas Médicas", "Laboratórios", "Despachantes", "Seguradoras", 
    "Gráficas", "Marcenarias", "Vidraçarias", "Serralherias", "Lojas de Móveis"
]

# CONFIGURAÇÕES DE FILTRO (Mude para True se quiser ativar)
FILTRAR_EMPRESAS_COM_SITE = False   # Se True, ignora empresas que já têm site
FILTRAR_APENAS_CELULAR = False      # Se True, ignora telefones fixos

async def extrair_leads(cidade):
    async with async_playwright() as p:
        print(f"\n[AutoFlow] Iniciando varredura completa em: {cidade}")
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        todos_leads = []
        telefones_processados = set()
        
        # CONFIGURAÇÃO DE SALVAMENTO IMEDIATO (BACKUP)
        os.makedirs('output', exist_ok=True)
        safe_cidade = re.sub(r'[\\/*?:"<>|]', "", cidade).replace(" ", "_")
        arquivo_backup = f"output/BACKUP_LEADS_{safe_cidade}.csv"
        
        # Cria o arquivo CSV com cabeçalho se ele não existir
        if not os.path.exists(arquivo_backup):
            pd.DataFrame(columns=["Nicho", "Empresa", "Telefone", "Endereço", "Cidade"]).to_csv(arquivo_backup, index=False, sep=';', encoding='utf-8-sig')
        else:
            # Carrega o que já foi salvo para não repetir (mesmo se reiniciar o script)
            try:
                df_bkp = pd.read_csv(arquivo_backup, sep=';', encoding='utf-8-sig')
                telefones_processados.update(df_bkp['Telefone'].astype(str).tolist())
                print(f"[AutoFlow] Memória carregada: {len(telefones_processados)} telefones já ignorados do backup.")
            except Exception: pass
        
        print(f"[AutoFlow] Backup em tempo real ativado: {os.path.abspath(arquivo_backup)}")

        for nicho in NICHOS_ALVO:
            termo_busca = f"{nicho} em {cidade}"
            try:
                # 1. Acessa o Google Maps
                await page.goto("https://www.google.com.br/maps", wait_until="commit", timeout=60000)
                
                # 2. Faz a pesquisa
                print(f"[AutoFlow] Pesquisando nicho: {nicho}")
                await page.wait_for_selector('input#searchboxinput', state="visible")
                await page.click('input#searchboxinput')
                await page.fill('input#searchboxinput', "")
                await page.type('input#searchboxinput', termo_busca, delay=100)
                await page.keyboard.press("Enter")
                
                # 3. SCROLL
                print(f"   Carregando TODOS os resultados (Isso vai demorar um pouco)...")
                try:
                    # Seleciona a caixa de resultados
                    feed = await page.wait_for_selector('div[role="feed"]', timeout=10000)
                    
                    last_count = 0
                    tentativas_sem_mudanca = 0
                    
                    while True:
                        # Rola diretamente via JavaScript (Mais rápido e confiável que mouse.wheel)
                        await feed.evaluate("element => element.scrollTop = element.scrollHeight")
                        await page.wait_for_timeout(2500) # Tempo maior para garantir carregamento
                        
                        # Conta quantos itens carregaram
                        itens = await page.query_selector_all('div[role="feed"] a[href*="/maps/place/"]')
                        count = len(itens)
                        
                        if count == last_count:
                            tentativas_sem_mudanca += 1
                        else:
                            tentativas_sem_mudanca = 0
                            print(f"      -> {count} empresas encontradas até agora...")
                        
                        if tentativas_sem_mudanca >= 5: # Se tentar 5x e não aparecer nada novo, para
                            print("   Fim da lista alcançado.")
                            break
                            
                        last_count = count
                        
                except Exception as e:
                    print(f"   [Aviso] Erro durante o scroll: {e}")

                # 4. Busca os links
                locais = await page.query_selector_all('div[role="feed"] a[href*="/maps/place/"]')
                print(f"   {len(locais)} locais encontrados.")

                for i, local in enumerate(locais):
                    try:
                        # FIX: Pega o nome direto do link (muito mais preciso que o H1 da página)
                        nome = await local.get_attribute("aria-label")
                        if not nome: nome = "Nome não encontrado"

                        await local.scroll_into_view_if_needed()
                        await local.click()
                        
                        # PAUSA DE SEGURANÇA: Espera 1s para a animação do clique acontecer
                        await page.wait_for_timeout(1000)
                        
                        # Espera o painel de detalhes carregar para garantir que pegamos os dados da empresa
                        try:
                            # Aumentei o timeout para 8s para evitar pular empresas em conexões lentas
                            await page.wait_for_selector('div[role="main"]', state="visible", timeout=8000)
                        except:
                            print(f"      [Erro] Timeout ao carregar detalhes de: {nome}")
                            continue
                        
                        # FILTRO: Verifica se a empresa já tem site
                        botao_site = await page.query_selector('a[data-item-id="authority"]')
                        if FILTRAR_EMPRESAS_COM_SITE and botao_site:
                            # Mostra o link encontrado para provar que o filtro está certo
                            link_site = await botao_site.get_attribute("href")
                            print(f"      [Pular] {nome} já possui site: {link_site}")
                            continue
                        
                        # Captura telefone
                        telefone = "Sem telefone"
                        try:
                            # Espera um pouco pelo botão de telefone para garantir que o painel carregou
                            # Aumentei para 5s
                            btn_tel = await page.wait_for_selector('button[data-item-id^="phone:"]', timeout=5000)
                            if btn_tel:
                                aria_lbl = await btn_tel.get_attribute("aria-label")
                                if aria_lbl:
                                    telefone = aria_lbl.replace("Telefone:", "").strip()
                        except:
                            pass # Se não tiver botão oficial, assume que não tem telefone

                        # FILTRO: Apenas Celular (WhatsApp) - Verifica se tem 11 dígitos e começa com 9 (após DDD)
                        digits = re.sub(r'\D', '', telefone)
                        if FILTRAR_APENAS_CELULAR and not (len(digits) == 11 and digits[2] == '9'):
                            print(f"      [Pular] {nome} possui telefone fixo: {telefone}")
                            continue
                        
                        # VERIFICAÇÃO DE DUPLICIDADE (Global)
                        if telefone in telefones_processados:
                            print(f"      [Duplicado] Telefone {telefone} já salvo anteriormente.")
                            continue
                        telefones_processados.add(telefone)

                        # Captura Endereço
                        endereco = "Endereço não encontrado"
                        btn_end = await page.query_selector('button[data-item-id="address"]')
                        if btn_end:
                            lbl_end = await btn_end.get_attribute("aria-label")
                            if lbl_end: endereco = lbl_end.replace("Endereço:", "").strip()
                        
                        # Dados do Lead
                        lead_atual = {
                            "Nicho": nicho,
                            "Empresa": nome, 
                            "Telefone": telefone,
                            "Endereço": endereco,
                            "Cidade": cidade
                        }
                        
                        todos_leads.append(lead_atual)
                        
                        # SALVA IMEDIATAMENTE NO CSV (Segurança contra falhas)
                        pd.DataFrame([lead_atual]).to_csv(arquivo_backup, mode='a', header=False, index=False, sep=';', encoding='utf-8-sig')
                        
                        print(f"      [+ LEAD] {nome} | {telefone}")
                        
                    except Exception:
                        continue
            
            except Exception as e:
                print(f"Erro ao processar nicho {nicho}: {e}")

        # 5. SALVAMENTO FINAL
        try:
            if not todos_leads:
                print("\n❌ Nenhum lead encontrado em nenhum nicho.")
            else:
                # Apenas confirmação, pois já foi salvo no loop
                nome_excel = f"output/RELATORIO_FINAL_{safe_cidade}.xlsx"
                pd.DataFrame(todos_leads).to_excel(nome_excel, index=False)
                print(f"\n✅ SUCESSO! Relatório Excel gerado: '{nome_excel}'")
                print(f"ℹ️  (Se der erro no Excel, seus dados estão seguros no arquivo CSV de backup)")
        except Exception as e:
            print(f"\nErro na finalização: {e}")

        await browser.close()

if __name__ == "__main__":
    cidade_alvo = input("Digite a cidade para mapear (Padrão: Marilia): ") or "Marilia"
    asyncio.run(extrair_leads(cidade_alvo))