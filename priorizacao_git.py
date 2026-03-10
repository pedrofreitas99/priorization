import os
import requests
import pandas as pd
from datetime import datetime

email = os.environ["JIRA_EMAIL"]
token = os.environ["JIRA_TOKEN"]
slack_bot_token = os.environ["SLACK_BOT_TOKEN"]
canal = os.environ["SLACK_CHANNEL_ID"]

dominio = "logcomex"
filtro_id = "12521"

campo_squad = "customfield_10038"
campo_modulo = "customfield_10033"

coluna_id_fake = "⠀"

url = f"https://{dominio}.atlassian.net/rest/api/3/search/jql"

payload = {
    "jql": f"filter={filtro_id}",
    "maxResults": 1000,
    "fields": [
        "summary",
        "status",
        "created",
        "assignee",
        "priority",
        campo_squad,
        campo_modulo,
        "issuetype"
    ]
}

# ==============================
# REQUISIÇÃO JIRA
# ==============================

response = requests.post(
    url,
    json=payload,
    auth=(email, token),
    headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
)

data = response.json()

if "issues" not in data:
    print("Erro retornado pela API:")
    print(data)
    raise SystemExit()

# ==============================
# EXTRAÇÃO DOS DADOS
# ==============================

tickets = []

for issue in data["issues"]:
    fields = issue["fields"]

    tipo_item = ""
    if fields.get("issuetype"):
        tipo_item = fields["issuetype"].get("name", "")

    chave = issue.get("key", "")
    resumo = fields.get("summary", "")

    status = ""
    if fields.get("status"):
        status = fields["status"].get("name", "")

    criado = fields.get("created", "")

    responsavel = ""
    if fields.get("assignee"):
        responsavel = fields["assignee"].get("displayName", "")

    prioridade = ""
    if fields.get("priority"):
        prioridade = fields["priority"].get("name", "")

    squad = ""
    if fields.get(campo_squad):
        if isinstance(fields[campo_squad], dict):
            squad = fields[campo_squad].get("value", "")
        else:
            squad = str(fields[campo_squad])

    modulo = ""
    if fields.get(campo_modulo):
        if isinstance(fields[campo_modulo], dict):
            modulo = fields[campo_modulo].get("value", "")
        else:
            modulo = str(fields[campo_modulo])

    tickets.append({
        "Tipo de item": tipo_item,
        "Chave": chave,
        "Data Abertura": criado,
        "Prioridade": prioridade,
        "Squad": squad,
        "Módulo": modulo,
        "Resumo": resumo,
        "Status": status,
        "Responsável": responsavel
    })

df = pd.DataFrame(tickets)

# ==============================
# FORMATAR DATA
# ==============================

if not df.empty:
    df["Data Abertura"] = pd.to_datetime(df["Data Abertura"], errors="coerce").dt.strftime("%d/%m/%Y")
    df["Data Abertura"] = df["Data Abertura"].fillna("")

# ==============================
# PRIORIZAR RUCKHABER / GREENWICH
# ==============================

df["prioridade_temp"] = df["Resumo"].astype(str).str.contains(
    "RUCKHABER|GREENWICH",
    case=False,
    na=False
)

df = pd.concat([
    df[df["prioridade_temp"]],
    df[~df["prioridade_temp"]]
]).drop(columns="prioridade_temp")

# ==============================
# LIMITAR E LIMPAR
# ==============================

df = df.fillna("")

# Slack table block aceita no máximo 100 linhas totais.
# Como a 1ª linha será o cabeçalho, sobraram 99 linhas de dados.
df = df.head(99).copy()

# Adiciona coluna "visualmente vazia" simulando ID sequencial
df.insert(0, coluna_id_fake, range(1, len(df) + 1))

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def limpar_texto(valor):
    texto = str(valor)

    texto = texto.replace("\n", " ")
    texto = texto.replace("\r", " ")
    texto = texto.replace("\t", " ")

    while "  " in texto:
        texto = texto.replace("  ", " ")

    return texto.strip()

def raw_text(texto):
    texto = str(texto)
    if texto == "":
        texto = " "
    return {
        "type": "raw_text",
        "text": texto
    }

# ==============================
# MONTAR TABELA NATIVA DO SLACK
# ==============================

colunas = [
    coluna_id_fake,
    "Tipo de item",
    "Chave",
    "Data Abertura",
    "Prioridade",
    "Squad",
    "Módulo",
    "Resumo",
    "Status",
    "Responsável"
]

rows = []

# Cabeçalho
rows.append([raw_text(col) for col in colunas])

# Linhas
for _, row in df.iterrows():
    rows.append([
        raw_text(limpar_texto(row[coluna_id_fake])),
        raw_text(limpar_texto(row["Tipo de item"])),
        raw_text(limpar_texto(row["Chave"])),
        raw_text(limpar_texto(row["Data Abertura"])),
        raw_text(limpar_texto(row["Prioridade"])),
        raw_text(limpar_texto(row["Squad"])),
        raw_text(limpar_texto(row["Módulo"])),
        raw_text(limpar_texto(row["Resumo"])),
        raw_text(limpar_texto(row["Status"])),
        raw_text(limpar_texto(row["Responsável"])),
    ])

# ==============================
# RESUMO POR MÓDULO
# ==============================

contagem_modulo = df["Módulo"].value_counts()

texto_modulos = "\n".join(
    [f"• {modulo}: {quantidade}" for modulo, quantidade in contagem_modulo.items()]
)

# ==============================
# ENVIAR PARA O SLACK COMO TABELA NATIVA
# ==============================

payload_slack = {
    "channel": canal,
    "text": f"Lista Priorização - {datetime.now().strftime('%d/%m/%Y')}",
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 Lista Priorização"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🗓️ *Data:* {datetime.now().strftime('%d/%m/%Y')}\n"
                    f"🎟️ *Total de Jiras:* {len(df)}\n\n"
                    f"📦 *Tickets por módulo:*\n{texto_modulos}"
                )
            }
        }
    ],
    "attachments": [
        {
            "blocks": [
                {
                    "type": "table",
                    "column_settings": [
    {"align": "left", "is_wrapped": False},  # ID
    {"align": "left", "is_wrapped": False},  # Tipo
    {"align": "left", "is_wrapped": False},  # Chave
    {"align": "left", "is_wrapped": False},  # Data
    {"align": "left", "is_wrapped": False},  # Prioridade
    {"align": "left", "is_wrapped": False},  # Squad
    {"align": "left", "is_wrapped": False},  # Módulo
    {"align": "left", "is_wrapped": False},  # Resumo
    {"align": "left", "is_wrapped": False},  # Status
    {"align": "left", "is_wrapped": False},  # Responsável
],
                    "rows": rows
                }
            ]
        }
    ]
}

resp_slack = requests.post(
    "https://slack.com/api/chat.postMessage",
    headers={
        "Authorization": f"Bearer {slack_bot_token}",
        "Content-Type": "application/json; charset=utf-8"
    },
    json=payload_slack
)

print(resp_slack.json())

