import hashlib
import os
import random
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class DionIndisponivel(Exception):
    pass


class SenhaInvalida(Exception):
    pass


CONFIG = {
    "latencia_ms": int(os.getenv("DION_LATENCIA_MS", "500")),
    "taxa_falha": float(os.getenv("DION_TAXA_FALHA", "0")),
}

class DionSimulado:
    DISCIPLINAS = ["Sistemas Distribuídos", "Engenharia de Software", "Banco de Dados II", "Compiladores"]
    AVALIACOES = ["P1", "P2", "Trabalho"]

    def _instabilidade(self):
        latencia_ms, taxa_falha = CONFIG["latencia_ms"], CONFIG["taxa_falha"]
        time.sleep(latencia_ms / 1000 * random.uniform(0.5, 1.5))
        if random.random() < taxa_falha:
            raise DionIndisponivel("falha simulada (DION_TAXA_FALHA)")

    def login(self, matricula: str, senha: str) -> None:
        self._instabilidade()
        if senha == "errada":
            raise SenhaInvalida(matricula)

    def buscar_notas(self, matricula: str, senha: str) -> list[dict]:
        self.login(matricula, senha)
        self._instabilidade()
        notas = []
        for d in self.DISCIPLINAS:
            for a in self.AVALIACOES:
                h = int(hashlib.sha256(f"{matricula}:{d}:{a}".encode()).hexdigest(), 16)
                if h % 4 == 0:
                    continue
                notas.append({"disciplina": d, "avaliacao": a, "valor": round(5 + (h % 51) / 10, 1)})
        return notas

class DionReal:

    MENU = re.compile(r"addSubmitParam\('([^']+)',\{'([^']+)':'([^']+)','([^']+_menuid)':'([^']+)'\}\)")
    DATA = re.compile(r"^\d{2}/\d{2}/\d{4}$")

    def __init__(self):
        self.base = os.getenv("DION_URL", "https://www.furb.br/dion").rstrip("/")
        self.timeout = float(os.getenv("DION_TIMEOUT_S", "20"))

    def _sessao(self, matricula: str, senha: str):
        s = requests.Session()
        s.headers["User-Agent"] = "FurbDylon/1.0 (trabalho academico de Sistemas Distribuidos)"
        try:
            s.get(f"{self.base}/aluno/index.xhtml", timeout=self.timeout)
            r = s.post(f"{self.base}/j_security_check",
                       data={"j_username": matricula, "j_password": senha}, timeout=self.timeout)
        except requests.RequestException as e:
            raise DionIndisponivel(str(e)) from e
        if r.status_code >= 500:
            raise DionIndisponivel(f"HTTP {r.status_code}")
        if 'name="j_password"' in r.text or "/aluno/" not in r.url:
            raise SenhaInvalida(matricula)
        return s, r

    def login(self, matricula: str, senha: str) -> None:
        self._sessao(matricula, senha)

    @staticmethod
    def _viewstate(soup, form_id):
        form = soup.find("form", id=form_id)
        campo = form and form.find("input", attrs={"name": re.compile(r"ViewState$")})
        if not campo:
            raise DionIndisponivel("ViewState não encontrado (layout do DION mudou?)")
        return form, campo["name"], campo["value"]

    def _disciplinas(self, soup):
        itens = []
        for a in soup.find_all("a", onclick=self.MENU):
            form_id, k1, v1, k2, menuid = self.MENU.search(a["onclick"]).groups()
            nome = a.get_text(" ", strip=True).rsplit(" - ", 1)[0]
            itens.append({"form": form_id, "params": {k1: v1, k2: menuid}, "semestre": int(menuid.split("_")[0]), "nome": nome})
        if not itens:
            return []
        atual = max(i["semestre"] for i in itens)
        return [i for i in itens if i["semestre"] == atual]

    def _notas_da_pagina(self, soup, disciplina):
        notas = []
        for tbody in soup.select("tbody.ui-datatable-data"):
            for tr in tbody.find_all("tr", recursive=False):
                cel = [td.get_text(" ", strip=True) for td in tr.find_all("td", recursive=False)]
                if len(cel) < 4 or not self.DATA.match(cel[0]) or not cel[3]:
                    continue
                try:
                    valor = float(cel[3].replace(",", "."))
                except ValueError:
                    continue
                notas.append({"disciplina": disciplina, "avaliacao": f"{cel[1]} ({cel[2]})", "valor": valor})
        return notas

    def buscar_notas(self, matricula: str, senha: str) -> list[dict]:
        s, r = self._sessao(matricula, senha)
        soup = BeautifulSoup(r.text, "html.parser")
        notas = []
        for d in self._disciplinas(soup):
            form, campo_vs, viewstate = self._viewstate(soup, d["form"])
            dados = {d["form"]: d["form"], campo_vs: viewstate, **d["params"]}
            try:
                r = s.post(urljoin(r.url, form.get("action") or r.url), data=dados, timeout=self.timeout)
            except requests.RequestException as e:
                raise DionIndisponivel(str(e)) from e
            if r.status_code >= 500:
                raise DionIndisponivel(f"HTTP {r.status_code}")
            soup = BeautifulSoup(r.text, "html.parser")
            notas.extend(self._notas_da_pagina(soup, d["nome"]))
        unicas = {(n["disciplina"], n["avaliacao"]): n for n in notas}
        return list(unicas.values())

def _instabilidade():
    time.sleep(CONFIG["latencia_ms"] / 1000 * random.uniform(0.5, 1.5))
    if random.random() < CONFIG["taxa_falha"]:
        raise DionIndisponivel("falha simulada (taxa de falha configurada)")

USUARIO_DE_TESTE = re.compile(r"^(f\d|carga-|escala-|queda-|pos-veneno-)")

class DionHibrido:

    def __init__(self):
        self.real = DionReal()
        self.simulado = DionSimulado()

    def _alvo(self, matricula):
        return self.simulado if USUARIO_DE_TESTE.match(matricula) else self.real

    def login(self, matricula, senha):
        alvo = self._alvo(matricula)
        if alvo is self.real:
            _instabilidade()
        alvo.login(matricula, senha)

    def buscar_notas(self, matricula, senha):
        alvo = self._alvo(matricula)
        if alvo is self.real:
            _instabilidade()
        return alvo.buscar_notas(matricula, senha)

def criar_dion():
    return DionHibrido() if os.getenv("DION_MODE", "real") == "real" else DionSimulado()
