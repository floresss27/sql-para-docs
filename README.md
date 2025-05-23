# Banco de Dados SQL para DOCS

- Gustavo das Flores Lima -> R.A: 22.221.041-1

### Instalação

Crie um novo ambiente virtual env de sua preferência.

Clone o repositório:

```
git clone git@github.com:floresss27/sql-para-neo4j.git
```

Ative o ambiente virtual.

- Instale as dependências:

  ```
  pip install psycopg2 python-dotenv pymongo
  ```

Configure suas variaveis de ambiente em um arquivo `.env` na raiz do projeto:

```
SQL_URL=""
MONGODB_URL=""
```

### Execução

Para executar a aplicação:

```
python app.py
```

Ao decorrer da execução, a aplicação irá criar as collections e inserir os dados no MongoDB. Você pode verificar os resultados na pasta `./resultados_docs` que será´criada automaticamente na raiz de seu projeto ao encerrar a execução.

1. `historico-escolar.json`: Historíco acadêmico de um aluno, com todas as disciplinas cursadas, semestre, ano, notas e média final.
2. `disc-professor.json`: Histórico de disciplinas ministradas por um professor específico, com o id da disciplina, nome da disciplina, semestre e ano.
3. `alunos-formados.json`: Lista de alunos graduados em um determinado semestre e ano.
4. `prof-chefes.json`: Lista de todos os professores que são chefes de departamento.
5. `grupo_tcc.json`: Lista de alunos que pertecem a um grupo de TCC específico e o nome do professor orientador.
