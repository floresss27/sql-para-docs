import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

conexao_mongo = MongoClient(os.environ['MONGODB_URL'])
banco_mongo = conexao_mongo.sqlparadocs

def buscar_historico_aluno():
    print("Recuperando histórico do aluno com RA: 241220555")

    consulta_pipeline = [
        {"$match": {"student_id": "241220555"}}, 
        {"$lookup": {
            "from": "subj",         
            "localField": "subj_id",
            "foreignField": "id", 
            "as": "detalhes_disciplina"  
        }},
        {"$unwind": "$detalhes_disciplina"}, 
        {"$project": {
            "student_id": 1,
            "subj_id": 1,
            "detalhes_disciplina.title": 1, 
            "semester": 1, 
            "year": 1,
            "grade": 1,
            "sala_disciplina": 1
        }}
    ]

    resultado = list(banco_mongo["takes"].aggregate(consulta_pipeline))

    for i, linha in enumerate(resultado):
        resultado[i]["_id"] = str(linha["_id"])
        resultado[i]["codigo_disciplina"] = linha["detalhes_disciplina"]["title"]
        resultado[i].pop("detalhes_disciplina")

    with open('./resultados_docs/historico-escolar.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

def disciplinas_professor():
    print("Buscando as disciplinas ministradas pelo professor de ID P010")

    pipeline = [
        {"$match": {"professor_id": "P010"}}, 
    ]

    resultado = list(banco_mongo["teaches"].aggregate(pipeline))

    for i, linha in enumerate(resultado):
        resultado[i]["_id"] = str(linha["_id"])

    with open('./resultados_docs/disc-professores.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

def alunos_formados():
    print("Buscando alunos formados no segundo semestre de 2018")

    pipeline = [
        {"$match": {"$and": [{"semester": 2}, {"year": 2018}]}}, 
    ]

    resultado = list(banco_mongo["graduate"].aggregate(pipeline))

    for i, linha in enumerate(resultado):
        resultado[i]["_id"] = str(linha["_id"])

    with open('./resultados_docs/alunos-formados.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

def chefes_departamento():
    print("Buscando os professores que são chefes de departamento")

    pipeline = [
        {"$lookup": {
            "from": "professor",         
            "localField": "boss_id",
            "foreignField": "id", 
            "as": "detalhes_professor"  
        }},
        {"$unwind": "$detalhes_professor"}, 
        {"$project": {
            "detalhes_professor": 1
        }}
    ]

    resultado = list(banco_mongo["department"].aggregate(pipeline))

    for i, linha in enumerate(resultado):
        detalhes_professor = linha["detalhes_professor"]
        resultado[i] = detalhes_professor
        resultado[i]["_id"] = str(resultado[i]["_id"])

    with open('./resultados_docs/prof-chefes.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

def grupo_de_tcc():
    print("Alunos participantes do grupo CC1234567")

    pipeline = [
        {"$match": {"group_id": "CC1234567"}},
        {"$lookup": {
            "from": "tcc_group",
            "localField": "group_id",
            "foreignField": "id",
            "as": "tcc_grupo"
        }},
        {"$unwind": "$tcc_grupo"},
        {"$lookup": {
            "from": "professor", 
            "localField": "tcc_grupo.professor_id", 
            "foreignField": "id",  
            "as": "detalhes_professor"
        }},
        {"$unwind": {"path": "$detalhes_professor", "preserveNullAndEmptyArrays": True}},
        {"$project": {  
            "id": 1,
            "name": 1,
            "course_id": 1,
            "group_id": 1,
            "nome_professor": "$detalhes_professor.name"
        }}
    ]

    resultado = list(banco_mongo["student"].aggregate(pipeline))

    if not resultado:
        print("Nenhum resultado encontrado para o grupo de TCC.")
        return

    registros = {
        "group_id": resultado[0].get("group_id", "Desconhecido"),
        "detalhes_professor": resultado[0].get("nome_professor", "Não informado"),
        "alunos": []
    }

    for linha in resultado:
        aluno = {
            "id": linha.get("id"),
            "name": linha.get("name"),
            "course_id": linha.get("course_id"),
        }
        registros["alunos"].append(aluno)

    with open('./resultados_docs/grupo-tcc.json', 'w') as f:
        json.dump(registros, f, ensure_ascii=False)