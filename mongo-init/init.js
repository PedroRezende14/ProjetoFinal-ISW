// init.js para MongoDB
db = db.getSiblingDB('appdb');

// Cria a collection mensagem (se não existir)
db.createCollection('mensagem', {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["telefone", "mensagem", "email"],
      properties: {
        telefone: {
          bsonType: "string",
          description: "Número de telefone deve ser uma string e é obrigatório"
        },
        mensagem: {
          bsonType: "string",
          description: "Conteúdo da mensagem deve ser uma string e é obrigatório"
        },
        email: {
          bsonType: "string",
          pattern: "^.+@.+\\..+$",
          description: "Email deve ser uma string válida e é obrigatório"
        },
        data_criacao: {
          bsonType: "date",
          description: "Data de criação da mensagem"
        }
      }
    }
  }
});

// Insere um documento inicial
db.mensagem.insertOne({
  telefone: "11999999999",
  mensagem: "Olá, mundo!",
  email: "teste@exemplo.com",
  data_criacao: new Date()
});

// Cria índice para melhorar buscas por telefone
db.mensagem.createIndex({ telefone: 1 });

// Cria índice para melhorar buscas por email
db.mensagem.createIndex({ email: 1 });