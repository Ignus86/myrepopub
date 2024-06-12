// index.js
const express = require('express');
const fs = require('fs');
const path = require('path');
const app = express();
const db = require('./config/db');

// Middleware per il parsing del body delle richieste
app.use(express.json());

// Funzione per leggere il file SQL
const readSQLFile = (filePath) => {
  return new Promise((resolve, reject) => {
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

// Rotta per ottenere tutti i record da una tabella
app.get('/api/records', async (req, res) => {
  try {
    const query = await readSQLFile(path.join(__dirname, 'query.sql'));
    db.query(query, (err, results) => {
      if (err) {
        console.error('Error fetching data:', err);
        res.status(500).json({ error: 'Failed to fetch data' });
        return;
      }
      res.json(results);
    });
  } catch (err) {
    console.error('Error reading SQL file:', err);
    res.status(500).json({ error: 'Failed to read SQL file' });
  }
});

// Avvia il server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
