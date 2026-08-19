const express = require('express');
const cors = require('cors');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
    res.send(`
        <html>
            <head>
                <title>Trading Bot Dashboard</title>
                <style>
                    body { background: #121212; color: white; text-align: center; padding: 50px; font-family: Arial; }
                    h1 { color: #089981; }
                    p { font-size: 18px; color: #aaaaaa; }
                </style>
            </head>
            <body>
                <h1>System is Online!</h1>
                <p>Dashboard is ready. We will add exchange forms here next.</p>
            </body>
        </html>
    `);
});

app.listen(PORT, () => {
    console.log('System Running on http://localhost:3000');
});
