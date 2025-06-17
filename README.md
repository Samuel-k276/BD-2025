# BD-2025 - Projeto de Bases de Dados

## Descrição Geral
Este repositório contém o projeto desenvolvido para a disciplina de Bases de Dados no ano letivo de 2025. O projeto é dividido em duas partes principais:

1. **Parte 1 (E1)**: Modelagem e criação do modelo relacional.
2. **Parte 2 (E2)**: Implementação de uma aplicação baseada em Flask para interagir com uma base de dados PostgreSQL.

## Estrutura do Repositório

- **E1/**: Contém os arquivos relacionados à primeira parte do projeto, incluindo o modelo relacional e o enunciado.
  - `algebra_relacional.pdf`: Documento com exercícios de álgebra relacional.
  - `modelo Relacional.docx`: Modelo relacional do projeto.
  - `Projeto_BD.drawio`: Diagrama do modelo relacional.

- **E2/**: Contém os arquivos da segunda parte do projeto.
  - `app/`: Código da aplicação Flask.
    - `app.py`: Código principal da aplicação.
    - `docker-compose.yml`: Configuração para execução com Docker.
    - `Dockerfile`: Configuração do container Docker.
    - `requirements.txt`: Dependências do projeto.
    - `testes.txt`: Comandos de teste para a API.
  - `data/`: Scripts SQL para criação e manipulação da base de dados.
    - `aviacao.sql`: Criação das tabelas.
    - `populate.sql`: Inserção de dados iniciais.
    - `index.sql`: Criação de índices.
    - `view.sql`: Criação de views.
    - `olap.sql`: Consultas OLAP.

- **README.md**: Este arquivo.


## Funcionalidades

### Endpoints da API
- `GET /`: Lista todos os aeroportos.
- `GET /voos/<partida>`: Lista voos a partir de um aeroporto específico.
- `GET /voos/<partida>/<chegada>`: Lista voos entre dois aeroportos.
- `POST /compra/<voo>`: Registra a compra de bilhetes para um voo.

### Base de Dados
A base de dados contém as seguintes tabelas principais:
- `aeroporto`: Informações sobre aeroportos.
- `aviao`: Informações sobre aviões.
- `assento`: Assentos disponíveis nos aviões.
- `voo`: Informações sobre voos.
- `venda`: Registro de vendas de bilhetes.
- `bilhete`: Informações sobre bilhetes emitidos.

### Consultas OLAP
O projeto inclui consultas OLAP para análise de dados, como:
- Taxa de ocupação de voos.
- Rotas mais populares.
- Estatísticas de vendas.

